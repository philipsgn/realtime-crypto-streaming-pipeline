---
name: crypto-pipeline
description: >
  Load this skill whenever the user asks about the Real-Time Crypto Streaming Pipeline project.
  Covers architecture, all 4 stages, tech stack, machine constraints, and code patterns.
  Trigger keywords: kafka, spark, binance, websocket, crypto, pipeline, stage, producer,
  streaming, timescaledb, grafana, vwap, window aggregation, docker compose, data engineer.
version: "1.0"
author: project-owner
references:
  - references/PROJECT_CONTEXT.md
  - references/STAGE_1_INGESTION.md
  - references/STAGE_2_PROCESSING.md
  - references/STAGE_3_STORAGE.md
  - references/STAGE_4_DASHBOARD_DEPLOY.md
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
├── dashboard/grafana/              ← Stage 4
├── infrastructure/docker-compose.yml
├── docs/PROJECT_CONTEXT.md         ← Context đầy đủ
└── docs/stages/                    ← Stage detail files
```

### RAM Budget (tổng ~1.2GB)

```
Kafka KRaft   : 350MB
PySpark local : 512MB
TimescaleDB   : 256MB
Grafana       : 200MB
Python scripts: ~100MB
```

### Stack không được thay đổi

Kafka ✓ | PySpark ✓ | TimescaleDB ✓ | Grafana ✓ | Binance WebSocket ✓

❌ Không dùng: Zookeeper, Flink, Redpanda, Superset, mock data

## Đọc thêm

Xem chi tiết từng stage trong thư mục `references/`.
