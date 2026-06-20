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
- .agents/skills/crypto-pipeline/references/*.md (mirrors docs/stages/)

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

Day 5 — AWS Migration (COST-OPTIMIZED — Free Tier only, single EC2 instance)
  - Kafka + Spark + PostgreSQL (self-managed, NOT RDS) all run on ONE
    EC2 t2.micro instance (Free Tier: 750 hours/month for 12 months) —
    consolidating to one instance avoids paying for multiple compute
    resources and avoids RDS's post-12-month billing cliff
  - Rationale to include in docs: RDS Free Tier expires after 12 months
    and RDS PostgreSQL does not support the TimescaleDB extension at all
    on managed tier — self-managed PostgreSQL+TimescaleDB on EC2 avoids
    both problems and costs nothing beyond the EC2 Free Tier hours
  - Parquet storage migrates from local disk to S3 bucket (S3 Free Tier:
    5GB storage, 20,000 GET + 2,000 PUT requests/month — sufficient for
    this project's data volume)
  - Grafana migrates to Grafana Cloud free tier (free forever tier: 10k
    series, 14-day retention) for a public dashboard URL for the CV
  - Add a cost-tracking note in docs: set up AWS Budgets alert at $1
    threshold so the student gets an email before anything could ever
    be charged

REQUIRED OUTPUT — update these files:

1. README.md
   - Add a "Roadmap" section with Day 1-5 above the existing 4-stage structure
   - Update "Project Structure" tree to include: dbt/, dags/ (Airflow), 
     ai/ (LLM summary script)
   - Update tech stack table to add: dbt, Airflow 2.9, Claude API, AWS 
     (EC2/S3/RDS/Grafana Cloud)
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
   - docs/stages/STAGE_8_AWS_MIGRATION.md
   Each file must follow the EXACT same structure as STAGE_1_INGESTION.md:
   Bối cảnh, Luồng dữ liệu, File liên quan, Cách chạy, Memory footprint,
   Lỗi thường gặp, Definition of Done, Skills học được — in Vietnamese,
   matching the existing tone and detail level.

5. Copy the 4 new stage files into:
   .agents/skills/crypto-pipeline/references/
   Update .agents/skills/crypto-pipeline/SKILL.md "references" frontmatter
   list to include all 4 new files.

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
- **Đổi Claude API → Gemini API free tier**: Gemini 2.5 Flash cho 1,500 requests/ngày miễn phí, không cần thẻ tín dụng — đủ dư cho nhu cầu gọi mỗi 5 phút (288 lần/ngày). CV vẫn ghi được "LLM-powered market intelligence", interviewer không quan tâm bạn dùng Claude hay Gemini.
- **AWS Day 5 tối ưu chi phí**: dùng 1 EC2 instance duy nhất chạy cả Kafka + Spark + PostgreSQL, bỏ RDS — vì RDS hết free tier sau 12 tháng và không hỗ trợ TimescaleDB extension. Thêm AWS Budget alert ở $1 để có email cảnh báo trước khi bị tính phí bất ngờ.
- Lấy Gemini API key tại **aistudio.google.com/apikey** — hoàn toàn miễn phí, không cần khai báo thẻ.
