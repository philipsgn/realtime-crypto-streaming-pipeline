# Stage 8 — AWS Migration

> **Mục tiêu cuối ngày 5:** Di chuyển pipeline sang AWS Free Tier tối ưu chi phí, giữ một EC2 duy nhất cho Kafka + Spark + PostgreSQL và có cảnh báo budget trước khi bị tính phí.

---

## Bối cảnh

Stage này giải quyết câu hỏi: *"Làm sao demo project lên cloud mà vẫn giữ chi phí thấp và không phá vỡ logic hiện tại?"*

AWS có nhiều service, nhưng không phải service nào đều phù hợp cho student project. Vì mục tiêu là **cost-optimize**, stage này ưu tiên:

- **1 EC2 t2.micro** chạy Kafka + Spark + PostgreSQL self-managed
- **S3** để lưu Parquet
- **Grafana Cloud free tier** để public dashboard
- **AWS Budgets alert ở $1** để tránh bị surprise charge

Điểm cốt lõi là tránh dùng RDS vì không hỗ trợ TimescaleDB extension và sẽ có chi phí sau thời gian free tier.

---

## Luồng dữ liệu Stage 8

```
[Local pipeline]
  Kafka + Spark + PostgreSQL
        │
        ▼
[Single EC2 instance]
  run all core services together
        │
        ├──▶ [S3 bucket]  → store Parquet
        ├──▶ [Grafana Cloud] → public dashboard URL
        └──▶ [AWS Budget alert] → email at $1
```

---

## File liên quan

| File | Vai trò |
|---|---|
| `infrastructure/docker-compose.yml` | Dùng như baseline cho deployment trên EC2 |
| `docs/stages/STAGE_5_DBT_TRANSFORMATION.md` | Giữ logic transformation ổn định sau migrate |
| `dashboard/grafana/` | Dùng để expose dashboard lên Grafana Cloud |
| `.env` | Điều chỉnh endpoint và credential cho cloud |

---

## Cách chạy Stage 8

### Bước 1 — Chuẩn bị EC2 Free Tier

- Chọn Ubuntu hoặc Amazon Linux
- Dùng `t2.micro` (nếu phù hợp Free Tier)
- Cài Docker, Python, Git, AWS CLI

### Bước 2 — Cấu hình S3 và budget alert

- Tạo bucket S3 cho Parquet
- Thiết lập AWS Budgets alert mức `$1`
- Đảm bảo bucket không lưu quá giới hạn free tier

### Bước 3 — Deploy pipeline lên EC2

```bash
# pull repo
git clone <repo-url>

# start services
make up
```

### Bước 4 — Connect Grafana Cloud

- Import datasource và dashboard
- Publish dashboard URL cho CV

---

## Memory footprint Stage 8

| Service | RAM dùng |
|---|---|
| EC2 single instance (Kafka + Spark + PostgreSQL) | ~1.0–1.3 GB |
| Grafana Cloud | external service |
| **Tổng Stage 8** | **phụ thuộc máy chủ cloud, không tính local RAM** |

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `SSH connection refused` | EC2 chưa bật port hoặc security group sai | Check inbound rules |
| `Kafka not reachable` | Advertised listener sai | Cấu hình đúng endpoint public/internal |
| `S3 permission denied` | IAM policy chưa đủ | Thêm quyền read/write cho bucket |
| `Grafana Cloud not loading` | Datasource URL sai | Kiểm tra endpoint và credentials |

---

## Definition of Done — Stage 8 hoàn thành khi

- [ ] Một EC2 instance chạy được pipeline core
- [ ] Parquet lưu thành công lên S3
- [ ] Grafana dashboard được public với URL ổn định
- [ ] Có AWS Budget alert ở mức $1
- [ ] Project vẫn chỉ dùng Free Tier resources và không có surprise charge

---

## Skills học được ở Stage này

- AWS basics: EC2, S3, IAM, Budget
- Cloud deployment mindset: cost-aware architecture
- Public dashboard hosting và demo setup
- Security và credential handling
- Trade-off giữa managed vs self-managed services
