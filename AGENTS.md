# AGENTS.md — Real-Time Crypto Streaming Pipeline

> Loaded automatically by Antigravity, Cursor, Claude Code at session start.
> Defines non-negotiable rules for every AI agent working on this project.

---

## Project Identity

This is a **Data Engineering portfolio project** built by a final-year university student
targeting a Data Engineer Intern position in Ho Chi Minh City, Vietnam.

- **Owner experience:** Beginner DE, strong Python basics
- **Goal:** Impress recruiters — not perfect production code
- **Timeline:** 5-day roadmap (Day 1-5), documentation-first before implementation
- **Real data only:** Binance WebSocket API, NO mock/simulated data ever

---

## Hard Rules (NEVER violate)

1. **NEVER suggest mock or simulated data** — project uses real Binance WebSocket stream
2. **NEVER suggest Zookeeper** — Kafka runs KRaft mode only
3. **NEVER suggest Flink or Redpanda** — user explicitly chose Kafka + PySpark
4. **NEVER suggest Superset** — replaced by Grafana
5. **NEVER exceed 1.5GB RAM in suggestions** — machine has only ~2GB free RAM
6. **ALWAYS use Python `os.getenv()`** for config — never hardcode credentials
7. **ALWAYS check RAM impact** before suggesting new Docker services
8. **Airflow standalone mode only — no Celery executor (RAM constraint)**
9. **dbt models read from TimescaleDB, never bypass Spark sink**
10. **AI summary stage uses GOOGLE GEMINI API (gemini-2.5-flash) ONLY — never Claude API or OpenAI API — this stage must remain $0 cost**
11. **AWS migration: single EC2 instance for Kafka+Spark+PostgreSQL, NOT RDS — RDS has no free TimescaleDB and bills after 12 months**
12. **Before implementing any AWS step, confirm it fits within Free Tier limits (EC2 750hrs/mo, S3 5GB, Grafana Cloud free tier) — flag any paid-tier requirement before writing code**
13. **dbt models read from TimescaleDB only via Silver/Gold layers — Grafana queries Gold tables, never queries trade_metrics_1min directly going forward**
14. **Airflow standalone/LocalExecutor only, max 768MB — never suggest CeleryExecutor or KubernetesExecutor for this project**

---

## Machine Constraints

```
CPU  : Intel i3-1115G4 @ 3.00GHz (2 cores, 4 logical processors)
RAM  : 7.7GB total, ~2GB free when Docker is running
Disk : SSD NVMe (fast I/O, no HDD concern)
OS   : Windows with Docker Desktop
```

### Approved memory limits per service

| Service | Max RAM | Config key |
|---|---|---|
| Kafka (KRaft) | 400MB | `KAFKA_HEAP_OPTS="-Xmx256m"` |
| PySpark | 512MB | `spark.driver.memory=512m` |
| PostgreSQL/TimescaleDB | 256MB | Docker deploy limit |
| Grafana | 200MB | Docker deploy limit |
| Airflow standalone | 768MB | Docker deploy limit |
| dbt | ~50MB run footprint | CLI process only |
| Python scripts | ~100MB each | — |

Note: the earlier 300MB Airflow estimate was too low because webserver gunicorn workers each need ~100-150MB, and LocalExecutor still runs scheduler, webserver, and executor overhead. A 600MB limit was tested but Airflow standalone was OOMKilled during startup, so the local limit is 768MB.

---

## Official Tech Stack

```
Language    : Python 3.10+
Ingestion   : websockets==12.0, kafka-python==2.0.2
Queue       : Apache Kafka 3.7 (KRaft, NO Zookeeper)
Processing  : PySpark 3.5 Structured Streaming, local[2] mode
Transformation: dbt Core (Bronze/Silver/Gold)
Storage     : TimescaleDB (PostgreSQL 16), Parquet (local → S3 later)
Orchestration: Apache Airflow 2.9 (standalone mode only)
AI          : Google Gemini API (gemini-2.5-flash, free tier)
Dashboard   : Grafana 10.4
Infra       : Docker Compose
CI/CD       : GitHub Actions (ruff + mypy)
```

---

## Code Style Rules

- **Type hints** on every function signature
- **Logging** via `logging` module — never `print()` in production code
- **Config** via `os.getenv("KEY", "default")` — never hardcoded
- **Error handling** with specific exceptions, not bare `except:`
- **Docstrings** on every function: one-line summary + Args/Returns if complex
- **Line length:** 100 chars max
- **Formatter:** ruff (not black, not flake8)

### Example of correct code pattern

```python
# CORRECT
import os
import logging

log = logging.getLogger(__name__)

def get_producer(bootstrap: str = None) -> KafkaProducer:
    """Create and return a configured Kafka producer."""
    servers = bootstrap or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    log.info(f"Connecting to Kafka at {servers}")
    return KafkaProducer(bootstrap_servers=servers)

# WRONG — never do this
def get_producer():
    return KafkaProducer(bootstrap_servers="localhost:9092")  # hardcoded!
```

---

## How to Load Project Context

When working on a specific stage, always reference the skill:

```
@crypto-pipeline tell me how Stage 1 works
@crypto-pipeline write the Kafka producer for Stage 1
@crypto-pipeline I'm getting OOM error in Stage 2, help me fix
```

Or reference specific stage docs:

```
@docs/stages/STAGE_1_INGESTION.md how do I verify Kafka is receiving events?
@docs/PROJECT_CONTEXT.md what metrics does Stage 2 compute?
```

---

## Stage Status Tracker

Update this section as you complete each stage:

| Stage | Status | Completed |
|---|---|---|
| Day 1 — Observability UI | ✅ Completed | Docker UI visibility added |
| Day 2 — dbt Transformation | ✅ Completed | Bronze/Silver/Gold models added |
| Day 3 — Airflow Orchestration | ✅ Completed | DAGs added, Airflow containerized |
| Day 4 — AI Market Summary | ✅ Completed | Resilient Gemini summaries and Grafana panel added |
| Day 5 — AWS Migration | 🔲 Not started | — |
| Stage 1 — Ingestion | 🔲 Not started | — |
| Stage 2 — Processing | 🔲 Not started | — |
| Stage 3 — Storage | 🔲 Not started | — |
| Stage 4 — Dashboard | 🔲 Not started | — |

---

## Common Questions & Correct Answers

**Q: Can I replace Kafka with Redis Streams to save RAM?**
A: No. Recruiter JDs require Kafka specifically. Use KRaft mode to reduce RAM instead.

**Q: Should I use Spark cluster mode?**
A: No. Use `local[2]` mode — cluster mode requires separate worker nodes, too heavy for this machine.

**Q: Can I use asyncio Kafka consumer instead of Spark?**
A: Only for debugging. Final pipeline must use PySpark Structured Streaming for CV credibility.

**Q: When should I move to cloud (AWS)?**
A: Stage 4 (Week 4) — deploy to AWS EC2 t2.micro Free Tier + S3 for Parquet storage.

---

## Day 4 AI Summary Resilience Rule

15. **AI summary stage must degrade gracefully to template fallback on Gemini quota
exhaustion — never let LLM API failure crash the DAG. Schedule is 30 min, not 5 min, by
design to preserve quota headroom against Google's history of free-tier cuts.**
