# Stage 8 - AWS Demo Runbook: Phase 0 Pre-flight

> **Mục tiêu:** Chuẩn bị đầy đủ tài khoản, network, source code, secrets và bằng chứng
> kiểm tra trước khi launch EC2. Phase 0 chỉ lập kế hoạch và xác minh; không tạo, sửa hoặc
> xóa bất kỳ tài nguyên AWS nào.

---

## Bối cảnh

Day 5 là production-grade cloud demo phục vụ CV trong phạm vi kiến trúc single-node có kiểm
soát chi phí. Môi trường chỉ chạy ngắn hạn, nhưng deployment, security, observability,
recovery checks và acceptance evidence phải đạt tiêu chuẩn đã định trước khi teardown.
Đây không phải workload 24/7 hoặc kiến trúc high availability. Pipeline dùng dữ liệu Binance
thật và giữ nguyên luồng:

```text
Binance WebSocket -> Kafka KRaft -> PySpark -> TimescaleDB
                  -> dbt Gold -> Airflow -> Gemini -> Grafana Cloud
                                      |
                                      +-> S3 Parquet
```

Tài khoản AWS được tạo tháng 01/2026, dùng AWS Free Plan dạng credit và còn khoảng `$42`.
Không áp dụng giả định Free Tier cũ `750 giờ/tháng trong 12 tháng`. Phiên demo dự kiến chạy
trên một `m7i-flex.large` tại `us-east-1`, tối đa 4 giờ, với mục tiêu dùng dưới `$1` credit.

Không launch EC2 nếu bất kỳ mục bắt buộc nào trong tài liệu này chưa được đánh dấu hoàn tất.
AWS Budgets chỉ cảnh báo và có thể cập nhật trễ; nó không phải hard spending cap.

---

## Thứ tự phase bắt buộc

Thực hiện đúng thứ tự sau. Không bỏ qua phase hoặc làm song song các phase có dependency:

| Phase | Công việc | Điều kiện chuyển phase |
|---|---|---|
| 1 | Tạo IAM Role cho EC2 truy cập S3 | Role tồn tại, không tạo access key |
| 2 | Tạo S3 bucket và chuẩn bị Spark ghi `s3a://` | Bucket private; Spark S3 code đã merge |
| 3 | Launch EC2 và cấu hình Security Group | Phase 1-2 hoàn tất; checklist Phase 0 đạt `GO` |
| 4 | Bootstrap EC2 qua SSH | Docker, Git, Java/Python và repository sẵn sàng |
| 5 | Chạy pipeline bằng EC2 Compose profile | Kafka, Postgres, Airflow và Spark healthy |
| 6 | Deploy bằng GitHub Actions `workflow_dispatch` | Deploy workflow chạy xanh và health check pass |
| 7 | Kết nối Grafana Cloud và thu thập bằng chứng | Dashboard có dữ liệu thật; ảnh đã được redact |

Ngay sau Phase 7 phải teardown toàn bộ workload AWS theo checklist Day 5. Không chỉ stop EC2:
phải terminate instance và xác minh EBS, S3, Security Group, IAM resource không còn phát sinh
chi phí.

---

## File liên quan

| File | Vai trò trong pre-flight |
|---|---|
| `infrastructure/docker-compose.yml` | Baseline local, network name và memory limits hiện tại |
| `ingestion/binance_producer.py` | Binance WebSocket -> Kafka, dùng `KAFKA_BOOTSTRAP_SERVERS` |
| `processing/spark_streaming.py` | Kafka -> TimescaleDB/Parquet; cần cloud-readiness trước EC2 |
| `dbt_project/` | Bronze/Silver/Gold và Gold serving models |
| `dags/` | Bốn DAG: dbt, lag monitor, daily report và AI summary |
| `ai/gemini_summary.py` | Gemini 2.5 Flash với fallback template |
| `.env.example` | Danh sách config; không chứa secret thật |

---

## Cách chạy Phase 0

### Bước 1 - Xác nhận không có AWS action

- [ ] Chưa launch EC2.
- [ ] Chưa tạo IAM Role, S3 bucket, Security Group hoặc key pair.
- [ ] Chưa chạy AWS CLI/Terraform/CloudFormation có khả năng thay đổi tài nguyên.
- [ ] Chỉ đọc code, chuẩn bị giá trị và ghi lại kế hoạch local.

### Bước 2 - Checklist tài khoản và chi phí

- [ ] Đăng nhập AWS Console thành công bằng đúng tài khoản demo.
- [ ] Mở Billing and Cost Management và xác nhận AWS Free Plan vẫn còn hiệu lực.
- [ ] Ghi lại chính xác ngày hết hạn Free Plan; credit còn lại không kéo dài ngày hết hạn.
- [ ] Chụp screenshot số dư Free Plan credit trước demo, đã che AWS account ID.
- [ ] Xác nhận số dư hiện tại xấp xỉ `$42` và đủ cho phiên demo tối đa 4 giờ.
- [ ] Chuẩn bị email nhận AWS Budgets actual/forecast alert ở ngưỡng `$1`.
- [ ] Đặt timer local 4 giờ; không phụ thuộc riêng vào AWS Budget alert.
- [ ] Xác nhận region duy nhất sử dụng là `us-east-1`.
- [ ] Xác nhận instance dự kiến là `m7i-flex.large` x86, 2 vCPU, 8 GiB RAM.

### Bước 3 - Checklist IP và network

- [ ] Truy cập `https://whatismyip.com` ngay trước phiên demo.
- [ ] Ghi current public IPv4 theo dạng `<CURRENT_PUBLIC_IP>/32`.
- [ ] Không tái sử dụng IP ghi từ phiên trước vì residential public IP có thể thay đổi.
- [ ] Security Group Phase 3 chỉ cho phép SSH `22` và Airflow `8080` từ IP `/32` này.
- [ ] Kafka `9092/9093` và PostgreSQL `5432/5433` không mở `0.0.0.0/0`.
- [ ] Outbound cho phép DNS, HTTPS và các kết nối cần thiết tới Docker Hub, Binance,
  Gemini, S3 và Grafana Cloud.

### Bước 4 - Checklist GitHub và source code

- [ ] Repository GitHub truy cập được và remote `origin` trỏ đúng project.
- [ ] Branch hiện tại là `main`.
- [ ] Tất cả code Day 1-4 đã commit và push lên `origin/main`.
- [ ] CI trên commit mới nhất đã chạy xanh.
- [ ] Không có secret, `.env`, key `.pem` hoặc AWS account ID trong Git history.
- [ ] Ghi commit SHA sẽ deploy để đối chiếu với GitHub Actions evidence.

Chạy local trước khi launch EC2:

```bash
git status --short
git branch --show-current
git fetch origin
git rev-parse HEAD
git rev-parse origin/main
git log -1 --oneline --decorate
```

Điều kiện pass: `git status --short` không có thay đổi cần deploy, branch là `main`, và hai
SHA từ `HEAD` và `origin/main` giống nhau. Tài liệu cá nhân chưa commit phải được commit,
stash hoặc xác nhận rõ là không thuộc artifact deploy; tuyệt đối không dùng `git reset --hard`.

### Bước 5 - Chuẩn bị `.env` an toàn

- [ ] Tạo file text tạm thời trên máy local, nằm ngoài repository và không sync public.
- [ ] File có đủ `KAFKA_BOOTSTRAP_SERVERS`, `GEMINI_API_KEY` và
  `POSTGRES_PASSWORD` để paste vào `.env` trên EC2.
- [ ] `POSTGRES_PASSWORD` là mật khẩu mới, không dùng `changeme`.
- [ ] `GEMINI_API_KEY` được kiểm tra còn hoạt động nhưng không xuất hiện trong log/screenshot.
- [ ] `PARQUET_OUTPUT` chứa đúng bucket name được tạo ở Phase 2.
- [ ] Không paste secret vào EC2 User Data, AWS Console tags/description, GitHub Actions
  input hoặc shell command có thể bị lưu vào history/log.
- [ ] Sau khi tạo `.env` trên EC2, chạy `chmod 600 .env` và xóa file text tạm sau demo.

Template chuẩn bị local, chỉ thay placeholder trong file tạm và không commit:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=crypto-trades
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT

POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=crypto_pipeline
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=<STRONG_RANDOM_PASSWORD>

PARQUET_OUTPUT=s3a://<PHASE_2_BUCKET_NAME>/raw-trades/
GEMINI_API_KEY=<REAL_GEMINI_API_KEY>
```

`localhost:9092` và `localhost:5433` dành cho producer/Spark chạy trên EC2 host. Airflow
trong Docker network phải tiếp tục dùng `kafka:9093` và `postgres:5432` qua Compose
environment override; không dùng một endpoint cho cả host process và container.

### Bước 6 - Verify Day 1-4 local

- [ ] Kafka KRaft healthy và topic `crypto-trades` có ba partitions.
- [ ] Producer nhận real Binance trades cho `BTCUSDT`, `ETHUSDT`, `SOLUSDT`.
- [ ] Spark ghi được `trade_metrics_1min`, `trade_metrics_5min` và Parquet local.
- [ ] `dbt run` và `dbt test` pass; Gold models có dữ liệu.
- [ ] Bốn DAG xuất hiện trong Airflow và latest run của từng DAG thành công.
- [ ] `kafka_lag_monitor_dag` fail khi broker unreachable, nhưng chỉ WARNING khi lag > 1000.
- [ ] `daily_summary_dag` fail khi query lỗi, nhưng chỉ WARNING khi không có dữ liệu hôm qua.
- [ ] AI DAG chạy mỗi 30 phút và ghi `source=gemini` hoặc `fallback_template`.
- [ ] Grafana local đọc Gold model cho volume và hiển thị AI summaries.

### Bước 7 - Cloud-readiness gate trước Phase 3

Các mục sau chưa có trong baseline local và phải được implement, review, test, commit và push
ở Phase 2/5/6 trước khi launch EC2:

- [ ] Spark không còn ép Windows `JAVA_HOME`, `.hadoop` hoặc driver bind `127.0.0.1`
  trong môi trường Linux EC2.
- [ ] Spark đọc `POSTGRES_PORT` từ environment thay vì hardcode `5433`.
- [ ] Spark có Hadoop AWS/S3A packages tương thích và ghi được `PARQUET_OUTPUT=s3a://...`.
- [ ] Spark checkpoint nằm trên EBS path ổn định và không bị xóa mỗi lần process restart.
- [ ] Có chiến lược compact/upload theo giờ để tránh S3 small files.
- [ ] Có `infrastructure/docker-compose.ec2.yml` hoặc Compose profile riêng; không chạy
  Grafana OSS, Kafdrop hoặc pgAdmin trên EC2.
- [ ] Airflow metadata connection dùng `POSTGRES_PASSWORD` từ environment, không hardcode
  `changeme`.
- [ ] EC2 bootstrap cài đủ Docker, Git, Java 17, Python và dependencies cần cho Spark host.
- [ ] Có `.github/workflows/deploy-ec2.yml` dùng `workflow_dispatch` và health checks.
- [ ] Có kế hoạch kết nối Grafana Cloud rõ ràng: ưu tiên PDC với read-only DB user; nếu dùng
  direct connection tạm thời thì chỉ allowlist Grafana Cloud egress CIDR chính thức.

### Quyết định GO/NO-GO

- [ ] **GO:** tất cả checkbox bắt buộc ở trên hoàn tất, SHA local khớp `origin/main`, CI xanh,
  Free Plan còn hiệu lực, IP vừa được kiểm tra và secrets đã chuẩn bị an toàn.
- [ ] **NO-GO:** chỉ cần một mục chưa đạt thì không launch EC2; sửa và verify local trước.

---

## Memory footprint dự kiến trên EC2

| Thành phần | Giới hạn/ước lượng | Ghi chú |
|---|---:|---|
| Kafka KRaft | 400-500 MiB | Giữ heap nhỏ, một broker |
| PySpark host | 512 MiB driver | `local[2]`, theo dõi thêm JVM overhead |
| PostgreSQL/TimescaleDB | 256-400 MiB | Một database cho data và Airflow metadata |
| Airflow standalone | 768 MiB | LocalExecutor, một webserver worker |
| Producer + dbt + PDC | khoảng 250-500 MiB | Không phải tất cả peak cùng lúc |
| OS, Docker và page cache | phần RAM còn lại | `m7i-flex.large` có 8 GiB |

Không đổi sang instance t-class chỉ để tiết kiệm credit trong phiên demo: workload Kafka,
Spark và Airflow có tải CPU liên tục, trong khi kế hoạch đã chọn `m7i-flex.large` để có đủ
RAM và không phụ thuộc CPU credits.

---

## Lỗi thường gặp

| Lỗi | Kiểm tra | Cách xử lý an toàn |
|---|---|---|
| EC2 không pull được Docker images | Kiểm tra Security Group outbound, route của public subnet tới Internet Gateway, DNS và HTTPS `443`; xác nhận IAM role đã attach | Sửa outbound/route/DNS rồi thử lại. IAM role cần cho S3 nhưng không cấp quyền pull image public từ Docker Hub; không tạo access key để chữa lỗi này |
| Airflow bị OOM/OOMKilled | Xác nhận instance thật sự là `m7i-flex.large`, không phải t-class; chạy `docker stats --no-stream`, `docker inspect airflow` và kiểm tra kernel OOM log | Giữ limit Airflow `768m`, một webserver worker và parallelism `1`; dừng Grafana OSS, Kafdrop, pgAdmin. Không tăng limit mù quáng trước khi xem container nào dùng RAM |
| Grafana Cloud không kết nối PostgreSQL | Kiểm tra PDC agent/status và read-only credential. Nếu direct connection, kiểm tra Security Group source CIDR có bị stale từ phiên trước hay không | Cập nhật đúng current allowed source: Grafana Cloud egress CIDR cho direct Cloud datasource; current public IP `/32` của sinh viên chỉ dùng khi test local/SSH/Airflow. Không mở PostgreSQL cho `0.0.0.0/0` |

---

## Definition of Done - Phase 0

- [ ] Đã đọc và đánh dấu toàn bộ checklist pre-flight.
- [ ] Đã ghi screenshot credit balance trước demo và ngày hết hạn Free Plan.
- [ ] Đã kiểm tra current public IP trong đúng phiên làm việc.
- [ ] Day 1-4 code đã push lên `main`, SHA local khớp `origin/main`, CI xanh.
- [ ] Secrets đã sẵn sàng trong file local tạm ngoài repository và không xuất hiện trong log.
- [ ] Tất cả cloud-readiness blockers đã có owner/phase xử lý trước Phase 3.
- [ ] Đã xác nhận thứ tự Phase 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7.
- [ ] Chưa có bất kỳ AWS resource nào được tạo trong Phase 0.
- [ ] Chỉ chuyển sang Phase 1 khi trạng thái cuối cùng là **GO**.

---

## Skills học được ở Stage này

- Pre-flight và go/no-go gate cho cloud deployment
- Quản lý secrets và credential boundary giữa host/container
- Kiểm soát chi phí bằng time limit, budget alert và teardown discipline
- Xác minh reproducibility bằng Git commit SHA và CI status
- Phân biệt network allowlist của client local và Grafana Cloud egress
