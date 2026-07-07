---
name: crypto-pipeline
description: >
  Load this skill whenever the user asks about the Real-Time Crypto Streaming Pipeline project.
  Covers architecture, all 8 stages of the roadmap, Docker automation, Azure demo deploy,
  tech stack, machine constraints, and code patterns.
  Trigger keywords: kafka, spark, binance, websocket, crypto, pipeline, stage, producer,
  streaming, timescaledb, grafana, vwap, window aggregation, dbt, airflow, gemini,
  docker compose, azure, data engineer.
version: "1.2"
author: project-owner
references:
  - ../../../docs/PROJECT_CONTEXT.md
  - ../../../docs/stages/STAGE_1_INGESTION.md
  - ../../../docs/stages/STAGE_2_PROCESSING.md
  - ../../../docs/stages/STAGE_3_STORAGE.md
  - ../../../docs/stages/STAGE_4_DASHBOARD_DEPLOY.md
  - ../../../docs/stages/STAGE_5_DBT_TRANSFORMATION.md
  - ../../../docs/stages/STAGE_6_AIRFLOW_ORCHESTRATION.md
  - ../../../docs/stages/STAGE_7_AI_MARKET_SUMMARY.md
  - ../../../docs/stages/STAGE_8_AZURE_DEMO_RUNBOOK.md
---

# Skill: crypto-pipeline

## Khi nào skill này được load?

Load skill này khi user hỏi bất kỳ điều gì liên quan đến:
- Cách code từng stage (Ingestion, Processing, Storage, Dashboard)
- Lỗi khi chạy Docker, Kafka, Spark, Grafana
- Câu hỏi về architecture hoặc design decision
- Viết, sửa, hoặc debug code trong project
- Giải thích khái niệm kỹ thuật trong context của project

## Quick Reference

### Cấu trúc project

```
realtime-crypto-streaming-pipeline/
├── AGENTS.md                        ← Rules cho AI agents
├── GEMINI.md                        ← Antigravity-specific rules
├── ingestion/binance_producer.py    ← Stage 1
├── processing/spark_streaming.py   ← Stage 2
├── storage/postgres_sink.py        ← Stage 3
├── dbt_project/                     ← Stage 5: Bronze/Silver/Gold
├── dags/                            ← Stage 6: Airflow DAGs
├── ai/                              ← Stage 7: Gemini summary
├── dashboard/grafana/              ← Stage 4
├── infrastructure/docker-compose.yml
├── docs/PROJECT_CONTEXT.md         ← Context đầy đủ
└── docs/stages/                    ← Stage detail files
```

### Giới hạn RAM theo service

```
Kafka KRaft   : 400MB
Producer      : 128MB
PySpark local : 512MB
Spark service : 768MB
TimescaleDB   : 256MB
Grafana       : 200MB
Airflow standalone/LocalExecutor: 768MB
dbt run       : ~50MB
Python scripts: ~100MB
```

### Docker automation hiện tại

```powershell
# Local full pipeline: Kafka, Postgres, Airflow, Producer, Spark, Grafana, Kafdrop, pgAdmin
docker compose -f infrastructure/docker-compose.yml up -d --build

# Azure demo: Kafka, Postgres, Airflow, Producer, Spark
docker compose -f infrastructure/docker-compose.azure.yml up -d --build

# Debug thủ công ngoài Docker
python ingestion/binance_producer.py
python processing/spark_streaming.py
```

### Env boundary

```
.env
  Dùng khi chạy Python script thủ công từ Windows host.
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  POSTGRES_HOST=localhost
  POSTGRES_PORT=5433

.env.docker
  Dùng bởi Docker Compose services, bị gitignore.
  KAFKA_BOOTSTRAP_SERVERS=kafka:9093
  POSTGRES_HOST=postgres
  POSTGRES_PORT=5432
```

Trong Docker network, Kafka internal listener là `kafka:9093`. Không đổi Docker services về
`kafka:9092` nếu chưa cập nhật `KAFKA_ADVERTISED_LISTENERS`, vì client trong container có thể
nhận advertised host/port sai.

### Persistence / restart rules

- `producer` và `spark` là Docker services có `restart: unless-stopped`.
- Demo bình thường không còn yêu cầu chạy producer hoặc Spark thủ công.
- Spark checkpoints phải nằm trong named volume `spark_checkpoints:/tmp/checkpoint`.
- Không tự xóa checkpoint khi service restart. Chỉ reset khi cố ý đặt `RESET_SPARK_STATE=true`.
- TimescaleDB data nằm trong named volume `postgres_data`.
- Không chạy `docker compose down -v` trừ khi cố ý xóa toàn bộ DB/checkpoint.

### Docker image notes

- `infrastructure/Dockerfile.producer` dùng `python:3.10-slim` và chỉ cài dependency nhẹ:
  `websockets`, `kafka-python-ng`, `python-dotenv`, `certifi`.
- `infrastructure/Dockerfile.spark` dùng `python:3.10-slim-bookworm`, OpenJDK 17,
  `pyspark==3.5.0`, `kafka-python-ng`, `psycopg2-binary`, `python-dotenv`.
- Không dùng `bitnami/spark:3.5.0`; tag đó đã fail khi build vì Docker Hub không resolve được.
- Spark build lần đầu tải PySpark khoảng 317MB nên có thể lâu; các lần sau dùng cache.

### Stack không được thay đổi

Kafka ✓ | PySpark ✓ | TimescaleDB ✓ | Grafana ✓ | Binance WebSocket ✓ | dbt ✓ | Airflow ✓ | Gemini ✓

❌ Không dùng: Zookeeper, Flink, Redpanda, Superset, mock data, Claude/OpenAI API

## Đọc thêm

Xem context và tài liệu chuẩn trong `../../../docs/`. Không tạo bản mirror trong skill;
`docs/` là single source of truth của project.
