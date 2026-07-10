@crypto-pipeline
Tôi đang bắt đầu Stage 1 - Ingestion.
File chính: ingestion/binance_producer.py
Máy: Windows, Docker Desktop, RAM ~2GB free.
Hãy hướng dẫn tôi từng bước chạy Stage 1,
bắt đầu từ lệnh docker compose up.

@crypto-pipeline
Tôi đang chạy Stage 1 - ingestion/binance_producer.py
Gặp lỗi này: [PASTE LỖI VÀO ĐÂY]
OS: Windows, RAM free: ~2GB, Kafka đang chạy: [có/chưa]
Hãy giải thích nguyên nhân và cách fix.

@crypto-pipeline
Giải thích cho tôi file ingestion/binance_producer.py
hoạt động như thế nào từng dòng, dùng ngôn ngữ
dễ hiểu cho sinh viên năm cuối chưa có kinh nghiệm DE.

Thứ tự ưu tiên thực tế
Bây giờ → Tuần 4: Hoàn thành project local trước. Không có base thì không có gì để mở rộng.
Sau tuần 4 — Hướng dễ nhất và trending nhất 2026: Thêm AI layer vào project hiện tại. Cụ thể là gọi Claude/GPT API mỗi 5 phút để tự động tóm tắt thị trường bằng ngôn ngữ tự nhiên từ data Spark đang tính — chỉ cần ~50 dòng code nhưng CV viết được "LLM-powered market intelligence". Đây là thứ nhà tuyển dụng 2026 đang tìm kiếm nhiều nhất.
Sau đó — AWS: Migrate từng service một. Kafka → MSK, Spark → EMR Serverless, DB → Timestream. Làm được cái này thì không còn là Intern nữa mà là Junior DE thật sự.

Day 1 :
Kafka UI + PostgreSQL UI Hiểu luồng data
Thêm Kafdrop vào docker-compose — xem topic, message, partition trực quan
Thêm pgAdmin — query TimescaleDB bằng UI, thấy data thật
RAM dùng thêm ~200MB — vẫn an toàn

Day 2 :
dbt Transformation Layer Medallion Architecture
Bronze model — raw data từ TimescaleDB
Silver model — clean + validate (loại bỏ outlier giá)
Gold model — VWAP hourly, daily summary cho dashboard
Bạn đã biết dbt từ project cũ → tốn ~3-4 tiếng

Day 3 :
Airflow DAG Orchestration
DAG 1: chạy dbt models mỗi giờ
DAG 2: health check Kafka lag mỗi 5 phút → alert nếu lag > 1000
DAG 3: daily report — query Gold table, gửi tóm tắt
Dùng Airflow 2.9 standalone mode — nhẹ hơn full setup

Day 4 :
AI Layer — LLM Market Summary Trending 2026
Query Gold table mỗi 5 phút lấy VWAP, volume, price change
Gọi Claude API → tóm tắt thị trường bằng tiếng Việt tự nhiên
Lưu summary vào PostgreSQL table mới: market_summaries
Hiển thị trong Grafana panel Text → tự refresh mỗi 5 phút

Day 5 :
AWS Migration Cloud Deploy
EC2 t2.micro (free) — chạy Kafka + Spark thay Docker local
S3 bucket — lưu Parquet thay local files
RDS PostgreSQL free tier — thay TimescaleDB local
Grafana Cloud free — public dashboard URL cho CV

Notice for run Docker :
docker compose -f infrastructure/docker-compose.yml up -d
docker compose -f infrastructure/docker-compose.yml up -d postgres grafana
docker compose -f infrastructure/docker-compose.yml ps
docker logs grafana --tail 100

# Giảm tải RAM
Restart nhanh khi UI chết:
docker compose -f infrastructure/docker-compose.yml restart airflow

Giảm tải RAM — tắt service không cần khi dùng Airflow:
docker compose -f infrastructure/docker-compose.yml stop pgadmin kafdrop

Nếu lặp lại thường xuyên — có thể bump memory lên 896m trong docker-compose.yml (cần tắt bớt container khác trước).

Lệnh test local
docker compose -f infrastructure/docker-compose.yml up -d --build
docker compose -f infrastructure/docker-compose.yml ps
docker compose -f infrastructure/docker-compose.yml logs producer --tail 20
docker compose -f infrastructure/docker-compose.yml logs spark --tail 20

Fix vĩnh viễn: Luôn tắt bằng:
docker compose -f infrastructure/docker-compose.yml down

docker exec postgres psql -U pipeline -d crypto_pipeline -c "SELECT COUNT(*) AS total_records, MIN(window_start), MAX(window_start) FROM trade_metrics_1min;"

docker compose -f infrastructure/docker-compose.yml stop spark
docker compose -f infrastructure/docker-compose.yml run --rm -e RESET_SPARK_STATE=true spark
docker compose -f infrastructure/docker-compose.yml up -d spark
