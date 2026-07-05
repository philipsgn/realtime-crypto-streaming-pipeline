# PROMPT — Cập nhật README.md + .agents/skills/crypto-pipeline cho 5-Day Roadmap

> Copy toàn bộ prompt dưới đây, paste vào Antigravity / Codex / Copilot Chat.
> Agent sẽ tự đọc file thật trên máy bạn trước khi sửa — không cần bạn paste code.

---

```
ROLE: You are a Senior Data Engineer with 5 years of experience reviewing
and extending a junior's portfolio project documentation.

CONTEXT: I have a working real-time crypto streaming pipeline:
Binance WebSocket -> Kafka -> PySpark Structured Streaming -> TimescaleDB -> Grafana,
with Kafdrop and pgAdmin already added for UI visibility.

The project currently has these doc files that need updating:
- README.md (root)
- AGENTS.md (root)
- docs/PROJECT_CONTEXT.md
- docs/stages/STAGE_1_INGESTION.md through STAGE_4
- .agents/skills/crypto-pipeline/SKILL.md
- .agents/skills/crypto-pipeline/SKILL.md references docs directly; do not create doc mirrors

TASK: I am extending this project over 5 new days. Read the existing
README.md, AGENTS.md, and docs/PROJECT_CONTEXT.md FIRST to understand
current state, current tech stack, and current file structure.
Then update them to reflect this NEW 5-day roadmap:

Day 1 — Observability UI (DONE — already implemented)
  - Kafdrop added to docker-compose.yml (Kafka topic/message/partition viewer)
  - pgAdmin added to docker-compose.yml (TimescaleDB query UI)
  - Mark this as completed in status tracking

Day 2 — dbt Transformation Layer (Medallion Architecture)
  - Bronze model: raw trade data from TimescaleDB trade_metrics_1min/5min
  - Silver model: cleaned + validated (remove price outliers, null checks)
  - Gold model: hourly VWAP rollup, daily summary aggregates for dashboard
  - dbt project connects to existing TimescaleDB (port 5433)

Day 3 — Airflow Orchestration (standalone mode, lightweight)
  - DAG 1: trigger dbt run hourly
  - DAG 2: Kafka consumer lag health check every 5 min, alert if lag > 1000
  - DAG 3: daily report — query Gold layer, summarize
  - Airflow 2.9 standalone (not full Celery executor — RAM constrained)

Day 4 — AI Layer: LLM Market Summary (COST-OPTIMIZED — $0)
  - Scheduled job queries Gold table every 5 min (VWAP, volume, price change)
  - Calls GOOGLE GEMINI API (gemini-2.5-flash model) — NOT Claude API, NOT
    OpenAI — because Gemini free tier gives 1,500 requests/day with no
    credit card required, which comfortably covers 288 calls/day (every
    5 min) with massive headroom
  - API key from https://aistudio.google.com/apikey — free, no billing setup
  - Stores result in new PostgreSQL table: market_summaries
  - New Grafana Text panel displays latest summary, auto-refresh 5 min
  - IMPORTANT: code must use python-dotenv to read GEMINI_API_KEY from .env,
    never hardcode the key, and must NOT reference Claude/Anthropic API
    anywhere in this stage — this stage is 100% free tier by design

Day 5 — AWS Ephemeral CV Demo (Free Plan credits, 4-hour hard cap)
  - Account context: AWS account created in January 2026, uses the new
    credit-backed AWS Free Plan, has about $42 credit remaining, and is NOT
    covered by the legacy 750 hours/month for 12 months offer
  - Before launching anything, verify the exact Free Plan expiration date in
    Billing and Cost Management; remaining credits do not extend that date
  - Deploy in us-east-1 on ONE m7i-flex.large x86 instance (2 vCPU, 8 GiB RAM)
    for a maximum of 4 hours; target total credit usage below $1
  - Run the full pipeline on EC2: Binance producer, Kafka KRaft, PySpark,
    self-managed PostgreSQL+TimescaleDB, dbt, Airflow standalone/LocalExecutor,
    and the Gemini market summary stage
  - Do NOT run pgAdmin, Kafdrop, or Grafana OSS on EC2; they are unnecessary
    for the CV demo and consume RAM
  - Do NOT use RDS: managed RDS PostgreSQL does not provide the required
    TimescaleDB setup. Do not repeat the obsolete 12-month Free Tier rationale
  - Do NOT use NAT Gateway, Load Balancer, Elastic IP, or any additional paid
    compute service; use a public subnet with an Internet Gateway
  - Security Group inbound rules: SSH 22 and Airflow 8080 from the student's
    current public IP only. Never expose Kafka 9092 or PostgreSQL 5432 publicly
  - Keep secrets in an EC2-local .env file with permission 600. Use an IAM
    instance role for AWS access; never store AWS access keys in code or .env
  - Migrate Parquet storage to a private S3 bucket with Block Public Access and
    SSE-S3 enabled. Keep Spark checkpoints on EBS and never delete checkpoints
    during application restart
  - Avoid S3 small-file growth: compact and upload Parquet hourly, partitioned
    by date/hour/symbol, targeting one closed data file per symbol per hour
  - Use Grafana Cloud free tier for the CV dashboard. Connect it to the private
    PostgreSQL datasource through Grafana Private Data Source Connect (PDC)
    with a dedicated read-only database user; do not expose port 5432
  - Import the existing dashboard, publish an externally shared dashboard URL,
    and verify that Gold metrics and the latest AI summaries are visible
  - Configure AWS Budgets actual and forecast alerts at $1 before EC2 launch.
    Document that Budget alerts can be delayed and are not a hard spending cap
  - Set EC2 instance-initiated shutdown behavior to Terminate and schedule an
    OS shutdown timer for 4 hours as a safety net
  - Collect CV evidence in the same session: architecture screenshot, healthy
    services, successful Airflow runs, S3 Parquet objects, Grafana Cloud public
    dashboard, and AWS credit balance before/after. Redact secrets and account ID
  - Definition of Done:
    * Real Binance events flow through Kafka -> Spark -> TimescaleDB -> dbt Gold
    * All four Airflow DAGs have a successful run on EC2
    * Compacted Parquet files exist in S3 under date/hour/symbol partitions
    * Grafana Cloud public dashboard shows Gold metrics and AI summaries
    * Recorded credit usage remains below the $1 target
    * All AWS workload resources are deleted within the same demo session
  - Teardown immediately after collecting evidence: terminate EC2; delete EBS
    volumes, snapshots/AMIs, S3 objects and bucket, dedicated IAM role/policy,
    Security Group, key pair, public IP/EIP, and Grafana PDC credentials
  - Final cost check: inspect EC2 Global View, S3, EBS volumes/snapshots, Elastic
    IPs, and Billing to confirm that no chargeable resources remain

REQUIRED OUTPUT — update these files:

1. README.md
   - Add a "Roadmap" section with Day 1-5 above the existing 4-stage structure
   - Update "Project Structure" tree to include: dbt_project/, dags/ (Airflow),
     ai/ (LLM summary script)
   - Update tech stack table to add: dbt, Airflow 2.9, Gemini API, AWS
     (EC2/S3/Grafana Cloud)
   - Keep existing Stage 1-4 content intact — append, do not delete

2. AGENTS.md
   - Add new Hard Rules: 
     * "Airflow standalone mode only — no Celery executor (RAM constraint)"
     * "dbt models read from TimescaleDB, never bypass Spark sink"
     * "AI summary stage uses GOOGLE GEMINI API (gemini-2.5-flash) ONLY —
       never Claude API or OpenAI API — this stage must remain $0 cost"
     * "AWS migration: single EC2 instance for Kafka+Spark+PostgreSQL,
       NOT RDS — RDS has no free TimescaleDB and bills after 12 months"
     * "Before implementing any AWS step, confirm it fits within Free
       Tier limits (EC2 750hrs/mo, S3 5GB, Grafana Cloud free tier) —
       flag any paid-tier requirement before writing code"
   - Update "Stage Status Tracker" table to add Day 1-5 rows
   - Update "Approved memory limits per service" table to add: 
     Airflow standalone (~300MB), dbt (~50MB run footprint)
   - Update "Official Tech Stack" section: add "AI: Google Gemini API
     (gemini-2.5-flash, free tier)" — explicitly do NOT list Claude API
     or OpenAI API since this project does not use paid LLM services

3. docs/PROJECT_CONTEXT.md
   - Update Architecture diagram (section 4) to show new flow:
     TimescaleDB -> dbt (Bronze/Silver/Gold) -> Airflow (orchestration) 
     -> AI summary -> Grafana
   - Add new terms to section 9 (Definitions): "Bronze/Silver/Gold", 
     "DAG", "lag" (Kafka consumer lag), "Free Tier"

4. Create NEW stage files (mirror existing STAGE_1/2 format exactly):
   - docs/stages/STAGE_5_DBT_TRANSFORMATION.md
   - docs/stages/STAGE_6_AIRFLOW_ORCHESTRATION.md
   - docs/stages/STAGE_7_AI_MARKET_SUMMARY.md
   - docs/stages/STAGE_8_AWS_DEMO_RUNBOOK.md
   Each file must follow the EXACT same structure as STAGE_1_INGESTION.md:
   Bối cảnh, Luồng dữ liệu, File liên quan, Cách chạy, Memory footprint,
   Lỗi thường gặp, Definition of Done, Skills học được — in Vietnamese,
   matching the existing tone and detail level.

5. Keep `docs/` as the single source of truth. Update
   `.agents/skills/crypto-pipeline/SKILL.md` frontmatter to reference
   `../../../docs/PROJECT_CONTEXT.md` and `../../../docs/stages/*.md` directly.
   Do not create or maintain mirrored Markdown files inside the skill.

CONSTRAINTS:
- Machine: Windows, Docker Desktop, RAM ~2GB free — flag any step 
  exceeding 300MB additional RAM
- COST: This entire roadmap must cost $0. AI layer uses Gemini API free
  tier (NOT Claude/OpenAI — those are pay-per-token with no meaningful
  free tier for sustained use). AWS layer uses Free Tier only, single
  EC2 instance, no RDS, with a $1 AWS Budget alert as a safety net.
- Do not invent file paths that don't match my actual project structure — 
  read the real files first
- Keep all existing content — this is an ADDITION, not a rewrite
- Vietnamese for explanations in docs/stages files, English for code 
  comments, consistent with existing files
- Do not start writing dbt/Airflow/AWS code yet — this task is 
  DOCUMENTATION ONLY, to plan before implementation
```

---

## Sau khi Agent trả lời

Yêu cầu Agent **diff từng file** trước khi apply — không để nó tự động overwrite:

```
Before applying changes, show me a diff/summary of what changed in 
each file. I will review and approve each file individually.
```

## Lưu ý quan trọng

- Prompt yêu cầu Agent đọc file thật trước — tránh tình trạng bịa nội dung không khớp code
- Tách riêng "documentation" và "implementation" — làm doc trước, code sau, để bạn review được lộ trình trước khi bắt tay code Day 2-5
- **Đổi Claude API → Gemini API free tier**: dùng Gemini 2.5 Flash mỗi 30 phút (48 lần/ngày) và template fallback khi quota/API lỗi. CV vẫn ghi được "LLM-powered market intelligence" mà không để lỗi LLM làm crash DAG.
- **AWS Day 5 tối ưu chi phí**: dùng một `m7i-flex.large` chạy full stack trong tối đa 4 giờ, không dùng RDS, thu bằng chứng CV rồi teardown toàn bộ. AWS Budget alert `$1` chỉ là cảnh báo, không phải hard spending cap.
- Lấy Gemini API key tại **aistudio.google.com/apikey** — hoàn toàn miễn phí, không cần khai báo thẻ.
