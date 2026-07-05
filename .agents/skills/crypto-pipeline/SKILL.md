---
name: crypto-pipeline
description: >
  Load this skill whenever the user asks about the Real-Time Crypto Streaming Pipeline project.
  Covers architecture, all 8 stages of the roadmap, tech stack, machine constraints, and code patterns.
  Trigger keywords: kafka, spark, binance, websocket, crypto, pipeline, stage, producer,
  streaming, timescaledb, grafana, vwap, window aggregation, dbt, airflow, gemini, docker compose, data engineer.
version: "1.1"
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
  - ../../../docs/stages/STAGE_8_AWS_DEMO_RUNBOOK.md
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
PySpark local : 512MB
TimescaleDB   : 256MB
Grafana       : 200MB
Airflow standalone/LocalExecutor: 768MB
dbt run       : ~50MB
Python scripts: ~100MB
```

### Stack không được thay đổi

Kafka ✓ | PySpark ✓ | TimescaleDB ✓ | Grafana ✓ | Binance WebSocket ✓ | dbt ✓ | Airflow ✓ | Gemini ✓

❌ Không dùng: Zookeeper, Flink, Redpanda, Superset, mock data, Claude/OpenAI API

## Đọc thêm

Xem context và tài liệu chuẩn trong `../../../docs/`. Không tạo bản mirror trong skill;
`docs/` là single source of truth của project.
