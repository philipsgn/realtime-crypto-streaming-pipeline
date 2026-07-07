# PROJECT CONTEXT — Real-Time Crypto Streaming Pipeline

> **Đọc file này trước khi trả lời bất kỳ câu hỏi nào liên quan đến project.**
> File này giúp AI Agent hiểu đúng bối cảnh, tech stack, constraints, và định hướng của toàn bộ project.

---

## 1. Mục tiêu project

Xây dựng một **end-to-end streaming data pipeline** xử lý dữ liệu giao dịch cryptocurrency real-time từ Binance, không dùng mock/simulated data. Pipeline phải phản ánh đúng cách một công ty fintech vận hành hệ thống data production.

**Người thực hiện:** Sinh viên năm cuối ngành CNTT, đang tự học để apply vị trí Data Engineer Intern tại TP. Hồ Chí Minh, Việt Nam.

**Mục đích chính:** Portfolio project triển khai theo production-oriented practices. Day 5 là
production-grade demo deployment chạy ngắn hạn trên Azure; sau khi kiểm thử và thu thập bằng
chứng CV thì teardown ngay để kiểm soát chi phí. Đây không phải workload phục vụ người dùng
thật 24/7 và kiến trúc một VM không được mô tả là high availability.

---

## 2. Constraints quan trọng (AI PHẢI GHI NHỚ)

| Constraint | Chi tiết |
|---|---|
| **Hardware** | Intel i3-1115G4, RAM 7.7GB (chỉ ~2GB free khi chạy), SSD NVMe |
| **OS** | Windows (Docker Desktop) |
| **Kinh nghiệm** | Sinh viên năm cuối, chưa có kinh nghiệm DE thực tế |
| **Thời gian** | Roadmap 5 ngày cho dbt, Airflow, AI và Azure demo sau baseline Stage 1–4 |
| **Budget** | Azure Free Account `$200 credit/30 ngày`; Day 5 chạy ngắn hạn rồi teardown |
| **Data** | Real data từ Binance WebSocket API, KHÔNG dùng mock/simulate |

> ⚠️ **Khi AI gợi ý giải pháp, phải luôn kiểm tra xem có phù hợp với RAM ~2GB free không. Nếu cần >1.5GB RAM thêm, phải nêu rõ và đề xuất cách tối ưu.**

---

## 3. Tech Stack chính thức

```
Ingestion   : Python (websockets, kafka-python) → Binance WebSocket API
Queue       : Apache Kafka 3.7 (KRaft mode, không dùng Zookeeper)
Processing  : PySpark 3.5 Structured Streaming (local[2] mode)
Transformation: dbt Core (Bronze / Silver / Gold)
Storage     : PostgreSQL + TimescaleDB (hypertable), Parquet (local → Azure Blob)
Orchestration: Apache Airflow 2.9 (standalone mode)
AI          : Google Gemini API (gemini-2.5-flash, free tier)
Dashboard   : Grafana 10.4
Cloud       : Một Azure VM Standard_B2s + Azure Blob + Managed Identity + Grafana Cloud
              (ephemeral CV demo)
Infra       : Docker Compose
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
        ├──▶ [Parquet files — local]
        │     Partition: date/symbol
        │     (Azure Blob khi deploy lên cloud)
        │
        └──▶ [dbt models — Bronze / Silver / Gold]
               Bronze: raw metrics from TimescaleDB
               Silver: clean + validate
               Gold: hourly/daily rollups
                      │
                      ▼
               [Apache Airflow — DAGs]
               DAG 1: dbt run hourly
               DAG 2: Kafka lag health check every 5 min
               DAG 3: daily summary report
               DAG 4: AI market summary every 30 min
                      │
                      ▼
               [AI summary job]
               Query Gold tables every 30 min
               Call Gemini API (gemini-2.5-flash)
               Save to market_summaries
                      │
                      ▼
               [Grafana Dashboard]
               Auto-refresh: 30s / 30 min summary panel
               Panels: BTC price, VWAP, volume bar, AI summary
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
├── dbt_project/
│   └── models/                   # Stage 5: Bronze / Silver / Gold models
├── dags/
│   ├── dbt_hourly_dag.py         # Stage 6: dbt deps/run/test
│   ├── kafka_lag_monitor_dag.py  # Stage 6: Kafka health
│   ├── daily_summary_dag.py      # Stage 6: daily Gold report
│   └── ai_market_summary_dag.py  # Stage 7: Gemini/fallback mỗi 30 phút
├── ai/
│   └── gemini_summary.py         # Stage 7: Gemini-based summaries
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
│       ├── STAGE_4_DASHBOARD_DEPLOY.md
│       ├── STAGE_5_DBT_TRANSFORMATION.md
│       ├── STAGE_6_AIRFLOW_ORCHESTRATION.md
│       ├── STAGE_7_AI_MARKET_SUMMARY.md
│       └── STAGE_8_AZURE_DEMO_RUNBOOK.md
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

## 7. Lộ trình 5 ngày

| Ngày | Stage | Mục tiêu cuối ngày | File chi tiết |
|---|---|---|---|
| 1 | Observability UI | Kafdrop + pgAdmin chạy được, thấy topic và data thật | STAGE_1_INGESTION.md |
| 2 | dbt Transformation | Bronze/Silver/Gold models chạy được trên TimescaleDB | STAGE_5_DBT_TRANSFORMATION.md |
| 3 | Airflow Orchestration | DAGs chạy dbt + health check Kafka lag | STAGE_6_AIRFLOW_ORCHESTRATION.md |
| 4 | AI Market Summary | Gemini summary lưu vào `market_summaries` và hiển thị Grafana | STAGE_7_AI_MARKET_SUMMARY.md |
| 5 | Azure Production-grade Demo | Full stack trên một VM Standard_B2s, Blob Storage + Grafana Cloud, thu bằng chứng rồi teardown | STAGE_8_AZURE_DEMO_RUNBOOK.md |

### How to Run

| Scenario | Command | `.env` file | Services started | Open in browser |
|---|---|---|---|---|
| Full pipeline local | `docker compose -f infrastructure/docker-compose.yml up -d --build` | `.env.docker` | Kafka, Postgres, Airflow, Producer, Spark, Grafana, Kafdrop, pgAdmin | `localhost:3000`, `localhost:9000`, `localhost:5050`, `localhost:8080` |
| Debug single script | `python ingestion/binance_producer.py` | `.env` | Only the script | — |
| Azure deploy | `docker compose -f infrastructure/docker-compose.azure.yml up -d --build` | `.env.docker` | Kafka, Postgres, Airflow, Producer, Spark | Grafana Cloud URL |

> ⚠️ Không chạy `docker compose down -v` trừ khi muốn xóa toàn bộ TimescaleDB data
> và Spark checkpoints. Dùng `docker compose down` để stop container nhưng giữ dữ liệu.

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
- Điểm cần hoàn thiện: Stage 8 production acceptance evidence và teardown evidence

### Khi user muốn mở rộng project
Gợi ý theo thứ tự ưu tiên:
1. Hoàn thành Azure VM + Blob Storage demo và production acceptance checks
2. Migrate toàn bộ Grafana analytics panels sang dbt Gold serving models
3. Thêm alert rule trong Grafana (price anomaly detection)
4. Thêm schema registry cho Kafka nếu RAM budget cho phép

---

## 9. Định nghĩa thuật ngữ cho AI

| Thuật ngữ | Ý nghĩa trong context này |
|---|---|
| "stage" | Một trong các giai đoạn của roadmap: Ingestion, Processing, Storage, Dashboard, dbt, Airflow, AI, Azure |
| "event" | Một giao dịch crypto từ Binance (JSON object) |
| "window" | Khoảng thời gian Spark gom events để tính metrics (1 min / 5 min) |
| "VWAP" | Volume-Weighted Average Price — giá trung bình có tính đến khối lượng |
| "hypertable" | Bảng TimescaleDB được tối ưu cho time-series data |
| "KRaft" | Kafka Raft — mode Kafka chạy không cần Zookeeper (từ Kafka 3.3+) |
| "watermark" | Ngưỡng thời gian Spark chờ late data trước khi finalize window |
| "sink" | Nơi Spark ghi output (PostgreSQL, Parquet, console...) |
| "Bronze/Silver/Gold" | Lớp dữ liệu medallion: raw → clean → analytics-ready |
| "DAG" | Directed Acyclic Graph — workflow định nghĩa task chạy theo thứ tự |
| "lag" | Khoảng chênh lệch giữa tốc độ sản xuất và tiêu thụ của Kafka consumer |
| "Azure Free Account" | Gói `$200 credit/30 ngày`; Standard_B2s dùng credit, không phải free forever, và phải teardown sau demo |
