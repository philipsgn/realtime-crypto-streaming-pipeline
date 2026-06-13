# PROJECT CONTEXT — Real-Time Crypto Streaming Pipeline

> **Đọc file này trước khi trả lời bất kỳ câu hỏi nào liên quan đến project.**
> File này giúp AI Agent hiểu đúng bối cảnh, tech stack, constraints, và định hướng của toàn bộ project.

---

## 1. Mục tiêu project

Xây dựng một **end-to-end streaming data pipeline** xử lý dữ liệu giao dịch cryptocurrency real-time từ Binance, không dùng mock/simulated data. Pipeline phải phản ánh đúng cách một công ty fintech vận hành hệ thống data production.

**Người thực hiện:** Sinh viên năm cuối ngành CNTT, đang tự học để apply vị trí Data Engineer Intern tại TP. Hồ Chí Minh, Việt Nam.

**Mục đích chính:** Portfolio project để gây ấn tượng với nhà tuyển dụng — không phải production system thực tế.

---

## 2. Constraints quan trọng (AI PHẢI GHI NHỚ)

| Constraint | Chi tiết |
|---|---|
| **Hardware** | Intel i3-1115G4, RAM 7.7GB (chỉ ~2GB free khi chạy), SSD NVMe |
| **OS** | Windows (Docker Desktop) |
| **Kinh nghiệm** | Sinh viên năm cuối, chưa có kinh nghiệm DE thực tế |
| **Thời gian** | 4 tuần, mỗi tuần 1 stage |
| **Budget** | Ưu tiên free tier — AWS Free Tier, Oracle Cloud Free |
| **Data** | Real data từ Binance WebSocket API, KHÔNG dùng mock/simulate |

> ⚠️ **Khi AI gợi ý giải pháp, phải luôn kiểm tra xem có phù hợp với RAM ~2GB free không. Nếu cần >1.5GB RAM thêm, phải nêu rõ và đề xuất cách tối ưu.**

---

## 3. Tech Stack chính thức

```
Ingestion   : Python (websockets, kafka-python) → Binance WebSocket API
Queue       : Apache Kafka 3.7 (KRaft mode, không dùng Zookeeper)
Processing  : PySpark 3.5 Structured Streaming (local[2] mode)
Storage     : PostgreSQL + TimescaleDB (hypertable), Parquet (local → S3)
Dashboard   : Grafana 10.4
Orchestration: Docker Compose
CI/CD       : GitHub Actions (ruff lint, mypy type check)
```

### Lý do chọn từng tool

- **Kafka KRaft** thay vì Kafka + Zookeeper → tiết kiệm ~300MB RAM
- **PySpark local[2]** thay vì Spark cluster → đủ cho demo, tiết kiệm RAM
- **TimescaleDB** thay vì PostgreSQL thường → tối ưu time-series query, Grafana native support
- **Grafana** thay vì Superset → chuẩn monitoring production, nhẹ hơn

### Stack KHÔNG dùng trong project này (tránh gợi ý)

- ❌ Zookeeper (đã thay bằng KRaft)
- ❌ Hadoop HDFS (quá nặng cho máy này)
- ❌ Flink (thay bằng Spark Structured Streaming theo yêu cầu JD)
- ❌ Redpanda (user muốn Kafka thật)
- ❌ Simulated/mock data (user muốn real data)
- ❌ Superset (đã thay bằng Grafana)

---

## 4. Architecture tổng quan

```
[Binance WebSocket API]
  wss://stream.binance.com:9443
  Symbols: BTCUSDT, ETHUSDT, SOLUSDT
  Event type: @trade (real-time trades)
        │
        ▼ JSON events, ~5-50 events/sec
[Apache Kafka — KRaft mode]
  Topic: crypto-trades
  Partitions: 3 (1 per symbol)
  Retention: 24h
        │
        ▼ Kafka consumer
[PySpark Structured Streaming]
  Tumbling window 1 min  → metrics: VWAP, volume, trade_count, price_change_%
  Tumbling window 5 min  → metrics: same + buy_sell_ratio
  Watermark: 10 seconds
        │
        ├──▶ [TimescaleDB / PostgreSQL]
        │    Tables: trade_metrics_1min, trade_metrics_5min
        │    Hypertable on: window_start
        │
        └──▶ [Parquet files — local]
             Partition: date/symbol
             (S3 khi deploy lên cloud)
                    │
                    ▼
             [Grafana Dashboard]
             Auto-refresh: 10s
             Panels: BTC price, VWAP, volume bar, trade heatmap
```

---

## 5. Cấu trúc thư mục project

```
realtime-crypto-streaming-pipeline/
├── ingestion/
│   └── binance_producer.py      # Stage 1: WebSocket → Kafka
├── processing/
│   └── spark_streaming.py       # Stage 2: Kafka → Spark → sinks
├── storage/
│   ├── postgres_sink.py         # Stage 3: TimescaleDB helpers
│   └── init.sql                 # Stage 3: Schema + hypertable DDL
├── dashboard/
│   └── grafana/provisioning/    # Stage 4: Datasource + dashboard as code
├── infrastructure/
│   └── docker-compose.yml       # Toàn bộ services, có memory limits
├── docs/
│   ├── PROJECT_CONTEXT.md       # ← File này
│   └── stages/
│       ├── STAGE_1_INGESTION.md
│       ├── STAGE_2_PROCESSING.md
│       ├── STAGE_3_STORAGE.md
│       └── STAGE_4_DASHBOARD_DEPLOY.md
├── .env.example
├── .gitignore
├── requirements.txt
├── Makefile
└── README.md
```

---

## 6. Metrics được tính toán

| Metric | Công thức | Window |
|---|---|---|
| `vwap` | Σ(price × qty) / Σ(qty) | 1 min, 5 min |
| `total_volume` | Σ(quantity) | 1 min, 5 min |
| `trade_count` | COUNT(*) | 1 min |
| `price_open` | FIRST(price) | 1 min |
| `price_close` | LAST(price) | 1 min |
| `price_change_pct` | (close - open) / open × 100 | 1 min |
| `buy_volume` | Σ(qty) where is_buyer_maker = false | 5 min |
| `buy_sell_ratio` | buy_volume / total_volume | 5 min |

---

## 7. Lộ trình 4 tuần

| Tuần | Stage | Mục tiêu cuối tuần | File chi tiết |
|---|---|---|---|
| 1 | Ingestion | Binance WebSocket → Kafka chạy được, thấy event vào topic | STAGE_1_INGESTION.md |
| 2 | Processing | Spark job đọc Kafka, tính VWAP, in ra console | STAGE_2_PROCESSING.md |
| 3 | Storage | Metrics ghi vào TimescaleDB, Parquet lưu raw | STAGE_3_STORAGE.md |
| 4 | Dashboard + Deploy | Grafana dashboard live, deploy Oracle/AWS | STAGE_4_DASHBOARD_DEPLOY.md |

---

## 8. Cách AI nên trả lời câu hỏi về project này

### Khi user hỏi về code
- Luôn dùng Python 3.10+, type hints, f-strings
- Không dùng deprecated API (kafka-python consumer loop cũ)
- PySpark: luôn dùng `foreachBatch` cho custom sinks, không dùng `foreach`
- Mọi config đều đọc từ `os.getenv()` hoặc `.env` file

### Khi user gặp lỗi
1. Hỏi xem lỗi xảy ra ở stage nào
2. Kiểm tra xem Docker container có đủ RAM không (`docker stats`)
3. Kiểm tra Kafka topic có nhận được message chưa trước khi debug Spark
4. Log level Spark nên để `WARN` để tránh noise

### Khi user hỏi về career / CV
- Project này target vị trí **Data Engineer Intern** tại HCM
- Điểm mạnh nhất để nhấn: real data (không mock), Kafka + Spark combo, end-to-end
- Điểm cần bổ sung thêm: dbt layer, AWS cloud deployment

### Khi user muốn mở rộng project
Gợi ý theo thứ tự ưu tiên:
1. Thêm dbt models (Bronze/Silver/Gold) → tăng DWH signal
2. Deploy AWS EC2 + S3 thật → tăng Cloud signal
3. Thêm alert rule trong Grafana (price anomaly detection)
4. Thêm schema registry (Confluent) cho Kafka → advanced

---

## 9. Định nghĩa thuật ngữ cho AI

| Thuật ngữ | Ý nghĩa trong context này |
|---|---|
| "stage" | Một trong 4 giai đoạn: Ingestion, Processing, Storage, Dashboard |
| "event" | Một giao dịch crypto từ Binance (JSON object) |
| "window" | Khoảng thời gian Spark gom events để tính metrics (1 min / 5 min) |
| "VWAP" | Volume-Weighted Average Price — giá trung bình có tính đến khối lượng |
| "hypertable" | Bảng TimescaleDB được tối ưu cho time-series data |
| "KRaft" | Kafka Raft — mode Kafka chạy không cần Zookeeper (từ Kafka 3.3+) |
| "watermark" | Ngưỡng thời gian Spark chờ late data trước khi finalize window |
| "sink" | Nơi Spark ghi output (PostgreSQL, Parquet, console...) |
