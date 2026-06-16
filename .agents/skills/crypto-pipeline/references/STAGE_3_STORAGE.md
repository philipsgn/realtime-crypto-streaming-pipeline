# Stage 3 — Storage (Lưu trữ dữ liệu)

Trong giai đoạn này, dữ liệu sau khi được tính toán bởi Spark Streaming sẽ được lưu trữ dài hạn để phục vụ việc truy vấn và trực quan hóa.

## 1. Công nghệ sử dụng
- **PostgreSQL**: Cơ sở dữ liệu quan hệ mã nguồn mở phổ biến nhất.
- **TimescaleDB**: Một extension (phần mở rộng) của PostgreSQL, biến nó thành một Time-Series Database (Cơ sở dữ liệu chuỗi thời gian) cực kỳ mạnh mẽ.

## 2. Cấu trúc bảng (Hypertable)
Thay vì lưu vào các bảng Postgres thông thường, chúng ta sử dụng **Hypertable** của TimescaleDB. 
Hypertable tự động chia nhỏ dữ liệu (partitioning) theo thời gian (ví dụ: mỗi tuần 1 chunk). Điều này giúp:
- Ghi dữ liệu tốc độ cao (High ingest rate) từ Spark.
- Truy vấn dữ liệu theo thời gian (ví dụ: vẽ biểu đồ giá trong 24h qua) cực kỳ nhanh.

File `storage/init.sql` chứa lệnh khởi tạo:
```sql
CREATE TABLE trade_metrics_1min (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    vwap DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    trade_count BIGINT,
    price_open DOUBLE PRECISION,
    price_close DOUBLE PRECISION,
    buy_volume DOUBLE PRECISION,
    price_change_pct DOUBLE PRECISION,
    window_minutes VARCHAR(20)
);

-- Chuyển thành Hypertable, phân mảnh theo cột window_start
SELECT create_hypertable('trade_metrics_1min', 'window_start');
```

## 3. Spark ghi vào Database như thế nào?
Trong file `spark_streaming.py`, Spark sử dụng `write.jdbc()` ở chế độ `append` (chỉ thêm mới) để đẩy dữ liệu vào Postgres thông qua giao thức JDBC. Mỗi khi một micro-batch (chu kỳ 30 giây) hoàn thành, hàm `write_to_postgres` sẽ được gọi:

```python
def write_to_postgres(batch_df, batch_id: int, table: str) -> None:
    if batch_df.isEmpty():
        return
    batch_df.write.jdbc(url=POSTGRES_URL, table=table, mode="append", properties=POSTGRES_PROPS)
```
