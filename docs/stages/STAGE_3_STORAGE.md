# Stage 3 — Storage (Lưu trữ dữ liệu)

Trong giai đoạn này, dữ liệu sau khi được tính toán bởi Spark Structured Streaming được lưu
vào TimescaleDB để truy vấn và Parquet để lưu trữ file.

## 1. Công nghệ sử dụng

- **PostgreSQL 16 + TimescaleDB**: lưu metrics time-series trong hypertable.
- **Parquet**: lưu dữ liệu theo định dạng cột; local ở môi trường development và Azure Blob ở Stage 8.

## 2. Cấu trúc bảng (Hypertable)

`storage/init.sql` tạo hai bảng `trade_metrics_1min`, `trade_metrics_5min`, chuyển chúng
thành hypertable theo `window_start` và thêm index `(symbol, window_start DESC)`.

```sql
CREATE TABLE IF NOT EXISTS trade_metrics_1min (
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

SELECT create_hypertable(
    'trade_metrics_1min',
    'window_start',
    if_not_exists => TRUE
);
```

## 3. Spark ghi dữ liệu như thế nào?

`processing/spark_streaming.py` dùng `foreachBatch` để ghi từng micro-batch vào PostgreSQL
bằng idempotent upsert (`ON CONFLICT (window_start, symbol) DO UPDATE`). Cấu hình kết nối
phải đọc từ environment; không hardcode credential trong code.

```python
def write_to_postgres(batch_df: DataFrame, table: str) -> None:
    """Upsert one non-empty Spark micro-batch to PostgreSQL."""
    if batch_df.isEmpty():
        return
    # INSERT ... ON CONFLICT (window_start, symbol) DO UPDATE
```

## 4. Definition of Done

- ✅ Hai bảng metrics tồn tại và là TimescaleDB hypertable.
- ✅ Spark ghi được micro-batch thật từ Binance vào cả window 1 phút và 5 phút.
- ✅ Restart Spark không tạo dữ liệu trùng ngoài chiến lược checkpoint đã định nghĩa.
- ✅ Parquet được ghi thành công vào output cấu hình bởi environment.
- ✅ Credential database không xuất hiện trong source code hoặc Git history.
