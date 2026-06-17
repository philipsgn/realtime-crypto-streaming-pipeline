# Stage 6 — Airflow Orchestration

> **Mục tiêu cuối ngày 3:** Tự động hóa các job quan trọng bằng Airflow standalone, giúp pipeline chạy theo lịch và có monitoring cơ bản.

---

## Bối cảnh

Stage này giải quyết câu hỏi: *"Làm sao chạy pipeline một cách có trật tự, tự động và dễ theo dõi?"*

Khi project có nhiều bước như dbt run, health check Kafka lag, báo cáo hàng ngày, việc chạy thủ công sẽ rất dễ sai. Airflow giúp định nghĩa workflows bằng DAG và chạy theo schedule.

Vì máy chỉ có khoảng 2GB RAM trống, stage này nên dùng **Airflow standalone mode** thay vì full Celery executor để tránh tiêu tốn RAM quá nhiều.

---

## Luồng dữ liệu Stage 6

```
[dbt models / Spark / DB]
        │
        ▼
[Airflow DAGs]
  DAG 1: trigger dbt run mỗi giờ
  DAG 2: check Kafka lag mỗi 5 phút
  DAG 3: daily report query Gold layer
        │
        ▼
[Logs + alerts + summaries]
  dễ theo dõi và debug
```

---

## File liên quan

| File | Vai trò |
|---|---|
| `dags/` | Chứa DAG definitions |
| `infrastructure/docker-compose.yml` | Khởi động các dịch vụ nền cần thiết |
| `docs/stages/STAGE_5_DBT_TRANSFORMATION.md` | Nền tảng cho DAG chạy dbt |
| `.env` | Config database, Kafka, email/notification nếu cần |

---

## Cách chạy Stage 6

### Bước 1 — Khởi động Airflow standalone

```bash
# Cài Airflow 2.9
pip install apache-airflow==2.9.0

# Khởi tạo metadata DB
airflow db init

# Tạo user admin
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Start webserver + scheduler
airflow webserver --port 8080
# terminal riêng
airflow scheduler
```

### Bước 2 — Đăng tải DAG

Đưa file DAG vào thư mục `dags/` và restart scheduler nếu cần.

### Bước 3 — Verify DAG chạy

Truy cập:

- `http://localhost:8080`
- Xem DAG state và logs

**Output mong đợi:**
```
DAG runs succeed
Task logs show dbt run / lag check completed
```

---

## Memory footprint Stage 6

| Service | RAM dùng |
|---|---|
| Airflow webserver + scheduler | ~250–300 MB |
| PostgreSQL metadata DB | ~100 MB |
| **Tổng Stage 6** | **~350–400 MB** |

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `ImportError` | DAG import sai module | Kiểm tra đường dẫn package và Python path |
| `Scheduler not picking up DAG` | File chưa được lưu đúng folder | Đặt DAG vào đúng thư mục `dags/` |
| `DB connection failed` | Metadata DB chưa init hoặc password sai | Chạy `airflow db init` lại |
| `Task timeout` | Query chạy quá lâu | Tối ưu SQL / tăng timeout hợp lý |

---

## Definition of Done — Stage 6 hoàn thành khi

- [ ] Airflow UI mở được và thấy DAGs
- [ ] Có ít nhất 3 DAGs được định nghĩa rõ ràng
- [ ] DAG chạy dbt thành công theo schedule
- [ ] Health check Kafka lag log ra đúng threshold
- [ ] Có dashboard/log để debug khi job fail

---

## Skills học được ở Stage này

- Airflow DAG và task scheduling
- Orchestration mindset: dependency và retry logic
- Monitoring và alerting cơ bản
- Quản lý workflow bằng logs và task state
- Tối ưu RAM cho local dev environment
