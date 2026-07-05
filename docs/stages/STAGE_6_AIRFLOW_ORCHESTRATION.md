# Stage 6 — Airflow Orchestration

> **Mục tiêu cuối ngày 3:** Tự động hóa các job quan trọng bằng Airflow standalone, giúp pipeline chạy theo lịch và có monitoring cơ bản.

---

## Bối cảnh

Stage này giải quyết câu hỏi: *"Làm sao chạy pipeline một cách có trật tự, tự động và dễ theo dõi?"*

Khi project có nhiều bước như dbt run, health check Kafka lag, báo cáo hàng ngày, việc chạy thủ công sẽ rất dễ sai. Airflow giúp định nghĩa workflows bằng DAG và chạy theo schedule.

Vì máy chỉ có khoảng 2GB RAM trống, stage này dùng **Airflow 2.9 standalone với
LocalExecutor**, một webserver worker, parallelism `1` và giới hạn container `768m`.
Không dùng CeleryExecutor hoặc KubernetesExecutor.

---

## Luồng dữ liệu Stage 6

```
[dbt models / Spark / DB]
        │
        ▼
[Airflow DAGs]
  dbt_hourly_dag          → dbt deps/run/test mỗi giờ
  kafka_lag_monitor_dag   → check Kafka lag mỗi 5 phút
  daily_summary_dag       → query Gold layer mỗi ngày
  ai_market_summary_dag   → Gemini/fallback mỗi 30 phút
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
| `infrastructure/docker-compose.yml` | Airflow standalone/LocalExecutor và memory limit |
| `infrastructure/Dockerfile.airflow` | Image Airflow 2.9 cùng dbt và Python dependencies |
| `docs/stages/STAGE_5_DBT_TRANSFORMATION.md` | Nền tảng cho DAG chạy dbt |
| `.env` | Config database, Kafka, email/notification nếu cần |

---

## Cách chạy Stage 6

### Bước 1 — Build và khởi động Airflow standalone

```bash
docker compose -f infrastructure/docker-compose.yml up -d --build airflow
```

Compose chạy `airflow standalone`, tự migrate metadata DB và tạo user cấu hình bằng
environment. Không khởi động thêm scheduler/webserver thủ công.

### Bước 2 — Đăng tải DAG

`dags/`, `ai/` và `dbt_project/` được mount vào container theo Compose. Kiểm tra DAG import:

```bash
docker exec airflow airflow dags list-import-errors
docker exec airflow airflow dags list
```

### Bước 3 — Verify DAG chạy

Truy cập:

- `http://localhost:8080`
- Xem DAG state và logs

Với image hiện tại, dùng `airflow dags list-runs`, `airflow tasks states-for-dag-run` và
đọc trực tiếp `/opt/airflow/logs/` khi cần task logs; lệnh `airflow tasks logs` không có.

---

## Memory footprint Stage 6

| Service | RAM dùng |
|---|---|
| Airflow standalone/LocalExecutor | container limit `768m` |
| Webserver workers | `1` |
| Parallelism | `1` |
| PostgreSQL metadata | dùng chung TimescaleDB container giới hạn 256 MB |

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `ImportError` | DAG import sai module | Kiểm tra đường dẫn package và Python path |
| `Scheduler not picking up DAG` | File chưa được lưu đúng folder | Đặt DAG vào đúng thư mục `dags/` |
| `DB connection failed` | Sai endpoint hoặc credential container | Dùng `postgres:5432` và environment Compose |
| `Task timeout` | Query chạy quá lâu | Tối ưu SQL / tăng timeout hợp lý |
| `OOMKilled` | Airflow vượt memory limit | Kiểm tra `docker stats`, inspect OOM và logs trước khi chỉnh limit |

---

## Definition of Done — Stage 6 hoàn thành khi

- [ ] Airflow UI mở được và thấy DAGs
- [ ] Cả bốn DAG import không lỗi và xuất hiện trong UI
- [ ] `dbt_hourly_dag` chạy `dbt deps/run/test` thành công
- [ ] Kafka lag task raise khi broker lỗi và chỉ WARNING khi lag vượt threshold
- [ ] Daily summary fail khi query lỗi, nhưng chỉ WARNING khi không có dữ liệu hôm qua
- [ ] AI summary chạy 30 phút/lần và không crash khi Gemini hết quota
- [ ] Có dashboard/log để debug khi job fail

---

## Skills học được ở Stage này

- Airflow DAG và task scheduling
- Orchestration mindset: dependency và retry logic
- Monitoring và alerting cơ bản
- Quản lý workflow bằng logs và task state
- Tối ưu RAM cho local dev environment
