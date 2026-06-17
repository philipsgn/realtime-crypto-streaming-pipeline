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
  Bronze  → raw source views / staging models
  Silver  → clean, dedupe, validate, remove outliers
  Gold    → hourly VWAP rollup, daily summary
        │
        ▼
[Analytics tables / dashboard inputs]
  ready for Grafana and downstream jobs
```

---

## File liên quan

| File | Vai trò |
|---|---|
| `dbt/` | Chứa project dbt, models và configs |
| `storage/init.sql` | Schema và table gốc để dbt đọc |
| `processing/spark_streaming.py` | Đảm bảo dữ liệu raw đã được sink đúng vào TimescaleDB |
| `.env` | Kết nối DB cho dbt |

---

## Cách chạy Stage 5

### Bước 1 — Chuẩn bị dbt project

```bash
# Cài dbt-core và adapter cho PostgreSQL
pip install dbt-core dbt-postgres

# Khởi tạo project dbt
dbt init crypto_pipeline
```

### Bước 2 — Viết models theo medallion

Các model nên có cấu trúc:

- `staging_` / `bronze_` để đọc bảng raw
- `silver_` để clean và validate
- `gold_` để tổng hợp theo giờ/ngày

### Bước 3 — Chạy dbt

```bash
# Chạy toàn bộ models
 dbt run

# Chạy test
 dbt test
```

**Output mong đợi:**
```
14:32:01  Running with dbt=1.8.0
14:32:02  Found 6 models, 3 tests
14:32:03  Finished running 6 models, 3 tests
```

---

## Memory footprint Stage 5

| Service | RAM dùng |
|---|---|
| dbt CLI | ~50 MB |
| PostgreSQL query load | ~100–150 MB |
| **Tổng Stage 5** | **~200 MB** |

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `relation does not exist` | Model đọc sai schema/table | Check tên table trong TimescaleDB |
| `connection refused` | DB chưa chạy hoặc port sai | Kiểm tra `docker ps` và `.env` |
| `duplicate rows` | Không dedupe đúng trong silver layer | Thêm unique key hoặc distinct logic |
| `outlier price` | Dữ liệu sai do Binance feed bất thường | Chặn giá ngoài phạm vi hợp lý |

---

## Definition of Done — Stage 5 hoàn thành khi

- [ ] dbt project chạy thành công không lỗi
- [ ] Có ít nhất 3 lớp models: Bronze, Silver, Gold
- [ ] Gold model cung cấp được hourly/daily aggregate phù hợp cho dashboard
- [ ] Có test cho null và schema validation
- [ ] Có thể query kết quả bằng SQL rõ ràng và dễ hiểu

---

## Skills học được ở Stage này

- dbt Core: sources, models, tests, snapshots
- Medallion architecture: Bronze/Silver/Gold
- SQL analytics: aggregations, rollups, window logic
- Data quality: validation, outlier handling, null checks
- DWH thinking: raw layer vs serving layer
