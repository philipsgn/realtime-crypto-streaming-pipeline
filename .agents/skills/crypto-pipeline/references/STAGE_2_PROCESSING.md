# Stage 2 — Stream Processing

> **Mục tiêu cuối tuần 2:** Spark job đọc từ Kafka, tính VWAP + volume theo window 1 phút, in kết quả ra console. Chưa cần ghi vào database.

---

## Bối cảnh

Stage này giải quyết câu hỏi: *"Làm sao biến raw events thành số liệu có ý nghĩa kinh doanh?"*

Kafka chỉ là buffer — nó lưu raw events nhưng không làm gì với chúng. Spark Structured Streaming là engine đọc từ Kafka, gom events theo thời gian (window), và tính các metrics như VWAP (giá trung bình có tính khối lượng), tổng volume giao dịch trong 1 phút vừa rồi.

Đây là phần **core nhất của pipeline** và cũng là phần nhà tuyển dụng hỏi nhiều nhất.

---

## Luồng dữ liệu Stage 2

```
[Kafka Topic: crypto-trades]
        │  SparkSession.readStream
        ▼
[Raw DataFrame]
  schema: binary key + binary value (JSON bytes)
        │  from_json() → parse schema
        ▼
[Parsed DataFrame]
  symbol, price, quantity, trade_time, is_buyer_maker...
        │  withWatermark("event_ts", "10 seconds")
        ▼
[Windowed Aggregation]
  groupBy(symbol, window("event_ts", "1 minute"))
  → vwap, total_volume, trade_count, price_open, price_close
        │
        ├──▶ Console sink (tuần 2 — debug)
        ├──▶ PostgreSQL sink via foreachBatch (tuần 3)
        └──▶ Parquet sink (tuần 3)
```

---

## Khái niệm quan trọng cần hiểu

### Tumbling Window là gì?

```
Timeline: ──────────────────────────────────────────▶
Events:      •  •   •    •  •  •    •   •     • •

Window 1min: [──── 07:33 ────][──── 07:34 ────][────...
             aggregate         aggregate
             vwap, vol          vwap, vol
```

Mỗi window 1 phút là một "ô thời gian" độc lập. Khi window đóng lại (sau 1 phút), Spark tính metrics cho toàn bộ events trong ô đó và output ra.

### Watermark là gì?

```
Thực tế network: event xảy ra lúc 07:33:58 nhưng đến Spark lúc 07:34:05 (late 7s)

Không có watermark: Spark đã close window 07:33 → event bị bỏ
Watermark 10s:      Spark chờ thêm 10s → event vẫn được tính vào window 07:33 ✓
```

Watermark = ngưỡng chịu đựng late data. Set 10 giây là đủ cho network latency bình thường.

### VWAP là gì?

```
VWAP = Volume-Weighted Average Price
     = Σ(price × quantity) / Σ(quantity)

Ví dụ 3 trades trong 1 phút:
  BTC @ 67,000 × 0.5 BTC = 33,500
  BTC @ 67,100 × 0.1 BTC =  6,710
  BTC @ 66,900 × 0.4 BTC = 26,760
  ─────────────────────────────────
  Tổng: 67,000 × (0.5+0.1+0.4) = (33,500+6,710+26,760) / 1.0 = 66,970

VWAP = 66,970 (phản ánh giá thật hơn simple average vì tính đến khối lượng)
```

---

## File liên quan

| File | Vai trò |
|---|---|
| `processing/spark_streaming.py` | Code chính của Stage 2 |
| `infrastructure/docker-compose.yml` | Kafka phải đang chạy |
| `requirements.txt` | pyspark==3.5.0 |

---

## Cách chạy Stage 2

### Prerequisites

```bash
# Kafka phải đang chạy từ Stage 1
docker ps | grep kafka  # phải thấy "healthy"

# Producer phải đang chạy (terminal riêng)
python ingestion/binance_producer.py
```

### Chạy Spark job (console mode — tuần 2)

```bash
make spark-job
# hoặc: python processing/spark_streaming.py
```

**Output mong đợi sau ~1 phút:**

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------+--------------------+--------+-----------+------------------+----------+
|symbol  |window_start        |vwap    |total_volume|trade_count       |price_change|
+--------+--------------------+--------+-----------+------------------+----------+
|BTCUSDT |2024-06-10 07:33:00 |67421.3 |2.4521     |47                |-0.23     |
|ETHUSDT |2024-06-10 07:33:00 |3518.7  |15.234     |31                |+0.11     |
|SOLUSDT |2024-06-10 07:33:00 |142.3   |892.1      |28                |+0.45     |
+--------+--------------------+--------+-----------+------------------+----------+
```

---

## Memory configuration quan trọng

```python
SparkSession.builder
  .config("spark.driver.memory", "512m")        # Giới hạn driver RAM
  .config("spark.sql.shuffle.partitions", "4")   # Mặc định 200 → quá nhiều cho máy này
  .master("local[2]")                            # Dùng 2 CPU cores, không tạo cluster
```

> ⚠️ **Không tăng `spark.driver.memory` quá 768m** trên máy RAM 8GB — sẽ bị OOM khi chạy cùng Kafka và PostgreSQL.

---

## Spark packages cần download

Khi chạy lần đầu, Spark tự download JARs (~150MB). Cần internet:

```python
.config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.postgresql:postgresql:42.7.3"
)
```

Lần đầu mất ~3-5 phút. Từ lần 2 trở đi dùng cache.

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `java.lang.OutOfMemoryError` | Spark dùng quá RAM | Giảm `spark.driver.memory` xuống 384m |
| `Kafka source offset not found` | Producer chưa gửi message | Chờ producer gửi 50+ events trước khi chạy Spark |
| `Schema mismatch` | JSON field name thay đổi | Check `parse_trade_event()` trong producer |
| Không thấy output sau 1 phút | Window chưa close | Chờ thêm 10-15s (watermark + trigger delay) |
| `ClassNotFoundException: kafka` | JAR chưa download | Chạy với internet, chờ Maven download |

---

## Definition of Done — Stage 2 hoàn thành khi

- [ ] Spark job chạy không crash trong 5 phút
- [ ] Console output thấy batches với VWAP, volume mỗi ~30 giây
- [ ] Có ít nhất 3 symbols (BTC, ETH, SOL) xuất hiện trong output
- [ ] `docker stats` CPU/RAM vẫn ổn định (không tăng liên tục)
- [ ] `price_change_pct` có cả giá trị dương và âm (chứng tỏ tính đúng)

---

## Skills học được ở Stage này

- Spark Structured Streaming: readStream, writeStream, trigger
- Window functions: tumbling window, watermark, late data
- Spark DataFrame API: `groupBy`, `agg`, `withColumn`
- VWAP computation: business metric trong fintech
- Memory tuning: JVM heap, shuffle partitions
- Kafka → Spark integration: `from_json`, schema inference
