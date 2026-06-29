# Real-Time Crypto Streaming Pipeline

> End-to-end streaming data engineering project — Binance WebSocket → Apache Kafka → Spark Structured Streaming → PostgreSQL → Grafana

![Architecture](docs/architecture.png)

## Overview

This project builds a **production-grade real-time data pipeline** that ingests live cryptocurrency trade events from Binance, processes them using Apache Kafka and Spark Structured Streaming, stores aggregated metrics in PostgreSQL (TimescaleDB), and visualizes them on a live Grafana dashboard.

**Domain:** Fintech / Market Data  
**Data source:** Binance WebSocket API (real trades, no simulation)  
**Latency:** Sub-minute (window aggregation every 1 minute)

---

## Roadmap (5 Days)

The project now extends beyond the original four stages with a lightweight 5-day roadmap for documentation and planning:

- **Day 1 — Observability UI (Done)**: add Kafdrop and pgAdmin so Kafka topics/messages and TimescaleDB data are visible in the browser.
- **Day 2 — dbt Transformation Layer**: build Bronze → Silver → Gold models for clean metrics and daily rollups.
- **Day 3 — Airflow Orchestration**: run dbt hourly and monitor Kafka lag with lightweight DAGs.
- **Day 4 — AI Market Summary (Done)**: generate resilient 30-minute Gemini summaries with a transparent template fallback.
- **Day 5 — AWS Migration**: run Kafka + Spark + PostgreSQL on one EC2 Free Tier instance, store Parquet in S3, and expose Grafana via Grafana Cloud.

> This roadmap is intended for planning first. The implementation work for Days 2–5 will come after documentation review.

---

## Architecture

```
Binance WebSocket API
        │  live trade events (BTC/USDT, ETH/USDT, SOL/USDT)
        ▼
Apache Kafka (KRaft mode)
  topic: crypto-trades
        │
        ▼
Spark Structured Streaming (PySpark)
  - Parse & validate JSON events
  - Tumbling window aggregation (1 min, 5 min)
  - Compute: VWAP, total volume, trade count, price change %
        │
        ├──▶ PostgreSQL (TimescaleDB extension)
        │      serving layer — Grafana reads here
        │
        ├──▶ dbt models (Bronze / Silver / Gold)
        │      clean metrics and rollups for analytics
        │
        └──▶ Local Parquet files
               historical storage — full raw events
                      │
                      ▼
               Grafana Dashboard
               BTC/ETH price, VWAP, volume heatmap (auto-refresh 10s)
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Python `websockets` | Connect to Binance WebSocket stream |
| Message Queue | Apache Kafka (KRaft) | Buffer & decouple producer/consumer |
| Stream Processing | PySpark Structured Streaming | Window aggregation, VWAP computation |
| Transformation | dbt Core | Bronze / Silver / Gold modeling |
| Serving DB | PostgreSQL + TimescaleDB | Time-series optimized storage |
| Historical Storage | Parquet (local / S3-ready) | Full raw event archive |
| Orchestration | Apache Airflow 2.9 (standalone) | Run dbt and monitoring DAGs |
| AI Layer | Google Gemini API (`gemini-2.5-flash`) | Generate free market summaries |
| Dashboard | Grafana | Real-time visualization |
| Cloud | AWS EC2 + S3 + Grafana Cloud | Cost-optimized deployment path |
| Infra | Docker Compose | Single-command local deployment |
| CI/CD | GitHub Actions | Linting, type check on push |

---

## Project Stages

### Stage 1 — Ingestion (Week 1)
Connect to Binance WebSocket and publish raw trade events to Kafka.

**What you build:**
- `ingestion/binance_producer.py` — WebSocket client for 3 symbols
- Kafka topic `crypto-trades` with KRaft (no Zookeeper)
- JSON schema: `symbol`, `price`, `quantity`, `trade_time`, `is_buyer_maker`

**Key learning:** Kafka producer API, KRaft mode, WebSocket async Python

---

### Stage 2 — Stream Processing (Week 2)
Consume from Kafka with Spark Structured Streaming and compute metrics.

**What you build:**
- `processing/spark_streaming.py` — PySpark job with two output streams
- Tumbling window 1 min: `VWAP`, `total_volume`, `trade_count`, `price_open`, `price_close`
- Tumbling window 5 min: same metrics for trend view
- Watermark: 10 seconds (handle late data)

**Key learning:** Spark watermarks, window functions, foreachBatch sink

---

### Stage 3 — Storage (Week 3)
Sink processed data into PostgreSQL (TimescaleDB) and raw events to Parquet.

**What you build:**
- `storage/postgres_sink.py` — write aggregated metrics to hypertable
- TimescaleDB hypertable on `trade_time` column
- Parquet sink: partitioned by `date/symbol`

**Key learning:** TimescaleDB hypertable, Spark JDBC sink, Parquet partitioning

---

### Stage 4 — Dashboard & Deploy (Week 4)
Build Grafana dashboard and deploy full stack.

**What you build:**
- Grafana provisioning: datasource + dashboard as code (JSON)
- 4 panels: BTC live price, ETH VWAP, volume bar chart, trade count heatmap
- Docker Compose: all services in one command
- Oracle Cloud Free Tier deployment (public demo URL)

**Key learning:** Grafana provisioning, Docker Compose networking, cloud deploy

---

## Quickstart

### Prerequisites
- Docker Desktop (4GB RAM allocated)
- Python 3.10+

### Run locally

```bash
# 1. Clone repo
git clone https://github.com/<your-username>/realtime-crypto-streaming-pipeline
cd realtime-crypto-streaming-pipeline

# 2. Copy env file
cp .env.example .env

# 3. Start all services
make up

# 4. Start Binance producer
make producer

# 5. Open Grafana
open http://localhost:3000   # admin / admin
# 6. Open observability UIs
# - Kafdrop (Kafka UI): http://localhost:9000
# - pgAdmin (Postgres UI): http://localhost:5050  — default: admin@crypto.com / admin
```

### How to run dbt

To run the dbt transformations (Bronze, Silver, Gold layers) against the TimescaleDB database:

```bash
cd dbt_project
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### How to run Airflow

To run the Airflow standalone container and view DAGs:

```bash
docker compose -f infrastructure/docker-compose.yml up -d airflow
```
- Access Airflow UI: http://localhost:8080
- Credentials: `admin` / `admin`

### Make commands

```bash
make up          # docker compose up (Kafka, Spark, PostgreSQL, Grafana)
make down        # docker compose down
make producer    # start Binance WebSocket producer
make spark-job   # submit Spark Structured Streaming job
make logs        # tail all container logs
make clean       # remove volumes and containers
```

---

## Project Structure

```
realtime-crypto-streaming-pipeline/
├── ingestion/
│   └── binance_producer.py      # Binance WebSocket → Kafka producer
├── processing/
│   └── spark_streaming.py       # PySpark Structured Streaming job
├── storage/
│   └── postgres_sink.py         # TimescaleDB write helpers
├── dbt/
│   └── models/                   # Bronze / Silver / Gold transformation
├── dags/
│   └── airflow/                  # Airflow DAGs for orchestration
├── ai/
│   └── gemini_summary.py         # Market summary script using Gemini API
├── dashboard/
│   └── grafana/
│       ├── datasource.yml        # Grafana datasource provisioning
│       └── dashboard.json        # Grafana dashboard as code
├── infrastructure/
│   ├── docker-compose.yml        # Full stack orchestration
│   └── kafka-kraft.properties    # Kafka KRaft config (no Zookeeper)
├── docs/
│   └── architecture.png
├── .env.example
├── .gitignore
├── requirements.txt
├── Makefile
└── README.md
```

---

## Key Metrics Computed

| Metric | Description | Window |
|---|---|---|
| `vwap` | Volume-Weighted Average Price | 1 min, 5 min |
| `total_volume` | Sum of trade quantities | 1 min, 5 min |
| `trade_count` | Number of trades | 1 min |
| `price_change_pct` | (close - open) / open × 100 | 1 min |
| `buy_sell_ratio` | buyer-initiated / total trades | 5 min |

---

## Environment Variables

```env
# .env.example
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=crypto-trades
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=crypto_pipeline
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=changeme

GRAFANA_ADMIN_PASSWORD=admin
```

---

## CV Description (copy-paste ready)

> **Real-Time Crypto Streaming Pipeline** — Built an end-to-end streaming data pipeline ingesting live trade events from Binance WebSocket API into Apache Kafka (KRaft), processed with PySpark Structured Streaming (VWAP, volume aggregation on 1-min tumbling windows), stored in TimescaleDB, and visualized on auto-refreshing Grafana dashboards. Containerized with Docker Compose and deployed on Oracle Cloud.

---

## Author

**[Your Name]**  
Data Engineering Portfolio — Ho Chi Minh City  
[LinkedIn](#) · [GitHub](#)

---

## Gemini AI Market Summary Setup

1. Open `https://aistudio.google.com/apikey` and create a Gemini API key.
2. Add the key to the project `.env` file: `GEMINI_API_KEY=your_key_here`.
3. For an existing PostgreSQL volume, apply the new idempotent schema from PowerShell:
   `Get-Content storage/init.sql -Raw | docker exec -i postgres psql -U pipeline -d crypto_pipeline`.
4. Rebuild and start Airflow: `docker compose -f infrastructure/docker-compose.yml up -d --build airflow`.
5. In Airflow, enable `ai_market_summary_dag`; it runs every 30 minutes.

The task generates at most one summary per symbol per run with `gemini-2.5-flash`. Only
HTTP 429 and 5xx responses are retried. If Gemini is unavailable, the task stores a
deterministic `fallback_template` summary instead of failing, and the `source` column makes
the degraded mode visible in PostgreSQL and Grafana.
