# Stage 5 — dbt Transformation

> **Mục tiêu cuối ngày 2:** Từ dữ liệu đã có trong TimescaleDB, xây dựng các model Bronze → Silver → Gold để dữ liệu dễ dùng cho dashboard và phân tích.

---

## Bối cảnh

Stage này giải quyết câu hỏi: *"Làm sao biến raw metrics thành dữ liệu sạch, có logic business và dễ dùng cho báo cáo?"*

Sau khi Spark đã ghi `trade_metrics_1min` và `trade_metrics_5min` vào TimescaleDB, dữ liệu vẫn còn là raw metrics. dbt giúp tổ chức logic transformation theo medallion architecture:

- **Bronze**: lấy raw data từ database
- **Silver**: loại bỏ null, validate schema, xử lý outlier
- **Gold**: rollup theo giờ/ngày để phục vụ dashboard và AI summary

Mục tiêu là tạo thêm lớp analytics rõ ràng, giúp recruiter thấy được tư duy data modeling.

---

## Luồng dữ liệu Stage 5

```
[TimescaleDB / PostgreSQL]
  tables: trade_metrics_1min, trade_metrics_5min
        │
        ▼
[dbt models]
  bronze_trades       → view đọc trade_metrics_1min
  silver_trades       → view lọc null, volume/price không hợp lệ và outlier
  gold_hourly_vwap    → table tổng hợp VWAP theo giờ
  gold_daily_summary  → table tổng hợp theo ngày
  gold_minute_volume  → view phục vụ volume theo phút cho Grafana
        │
        ▼
[Analytics tables / dashboard inputs]
  ready for Grafana and downstream jobs
```

---

## File liên quan

| File | Vai trò |
|---|---|
| `dbt_project/` | Chứa dbt project, profiles, models và tests |
| `storage/init.sql` | Schema và table gốc để dbt đọc |
| `processing/spark_streaming.py` | Đảm bảo dữ liệu raw đã được sink đúng vào TimescaleDB |
| `.env` | Kết nối DB cho dbt |

---

## Cách chạy Stage 5

### Bước 1 — Cài dependency

```bash
pip install dbt-core dbt-postgres
```

### Bước 2 — Kiểm tra cấu trúc hiện tại

Project hiện có:

- `models/bronze/bronze_trades.sql`
- `models/silver/silver_trades.sql`
- `models/gold/gold_hourly_vwap.sql`
- `models/gold/gold_daily_summary.sql`
- `models/gold/gold_minute_volume.sql`
- schema tests trong `models/schema.yml` và singular tests trong `tests/`

### Bước 3 — Chạy dbt

```bash
cd dbt_project
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Khi chạy trong Airflow container, profile nhận `POSTGRES_HOST=postgres` và
`POSTGRES_PORT=5432`. Khi chạy từ Windows host, giá trị mặc định là
`localhost:5433`. Cả hai đều phải đi qua `env_var()` trong `profiles.yml`.

---

## Memory footprint Stage 5

| Service | RAM dùng |
|---|---|
| dbt CLI | ~50 MB |
| PostgreSQL query load | dùng chung container TimescaleDB giới hạn 256 MB |
| **Tổng tăng thêm khi chạy dbt** | **~50 MB** |

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `relation does not exist` | Model đọc sai schema/table | Check tên table trong TimescaleDB |
| `connection refused` | Dùng nhầm endpoint host/container | Host dùng `localhost:5433`; Airflow dùng `postgres:5432` |
| `duplicate rows` | Không dedupe đúng trong silver layer | Thêm unique key hoặc distinct logic |
| `outlier price` | Dữ liệu sai do Binance feed bất thường | Chặn giá ngoài phạm vi hợp lý |

---

## Definition of Done — Stage 5 hoàn thành khi

- [ ] dbt project chạy thành công không lỗi
- [ ] `dbt debug`, `dbt run` và `dbt test` đều pass
- [ ] Có đủ `bronze_trades`, `silver_trades` và ba Gold models hiện tại
- [ ] Gold models cung cấp hourly VWAP, daily summary và minute volume cho dashboard
- [ ] Schema tests và singular tests đều pass
- [ ] Có thể query kết quả bằng SQL rõ ràng và dễ hiểu

---

## Skills học được ở Stage này

- dbt Core: sources, models, tests, snapshots
- Medallion architecture: Bronze/Silver/Gold
- SQL analytics: aggregations, rollups, window logic
- Data quality: validation, outlier handling, null checks
- DWH thinking: raw layer vs serving layer
