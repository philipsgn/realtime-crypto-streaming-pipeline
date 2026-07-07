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

Day 5 — Azure Ephemeral CV Demo ($200 credit, 30-day window)
  - Account context: Azure Free Account provides $200 credit for the first
    30 days. Use the credit window only; do not upgrade to Pay-as-you-go for
    this portfolio demo
  - Before launching anything, verify remaining credit and expiration date in
    Azure Cost Management. Credit is time-limited and does not make paid
    resources free forever
  - Deploy in Southeast Asia on ONE Standard_B2s VM (2 vCPU, 4GB RAM) for a
    short demo session. Do NOT use B1s because 1GB RAM is insufficient for
    Kafka + Spark + PostgreSQL + Airflow
  - Run the full pipeline on the Azure VM: Binance producer, Kafka KRaft,
    PySpark host process, self-managed PostgreSQL+TimescaleDB, dbt, Airflow
    standalone/LocalExecutor, and the Gemini market summary stage
  - Do NOT run pgAdmin, Kafdrop, or Grafana OSS on the Azure VM; they are not
    needed for the CV demo and consume RAM
  - Use Azure Blob Storage / ADLS Gen2 for Parquet output. Prefer
    `abfss://raw-trades@<account>.dfs.core.windows.net/` with Managed
    Identity; never commit storage keys
  - Use a user-assigned Managed Identity with Storage Blob Data Contributor on
    the storage account. Do not use committed access keys
  - Network Security Group inbound rules: SSH 22 and Airflow 8080 from the
    student's current public IP only. Never expose Kafka 9092 or PostgreSQL
    5432 publicly
  - Use Azure auto-shutdown as the safety net, set no later than 8 hours after
    planned start. Auto-shutdown is a safety guard, not a substitute for manual
    teardown
  - Keep secrets in a VM-local .env file with permission 600. Do not paste
    secrets into GitHub Actions inputs, Azure tags/descriptions, or committed
    files
  - Use Grafana Cloud free tier for the CV dashboard. If a temporary direct
    PostgreSQL connection is needed, allowlist only official Grafana Cloud
    egress CIDRs and remove the NSG rule during teardown
  - GitHub Actions deploy workflow is workflow_dispatch only:
    .github/workflows/deploy-azure.yml with input azure_vm_ip and secret
    AZURE_SSH_KEY
  - Collect CV evidence in the same session: architecture screenshot, healthy
    services, successful Airflow runs, Azure Blob Parquet objects, Grafana
    Cloud dashboard, and Azure Cost Management before/after screenshots.
    Redact secrets and account/subscription IDs
  - Definition of Done:
    * Real Binance events flow through Kafka -> Spark -> TimescaleDB -> dbt Gold
    * All four Airflow DAGs have a successful run on Azure VM
    * Parquet files exist in Azure Blob private container
    * Grafana Cloud dashboard shows Gold metrics and AI summaries
    * Azure resources are torn down immediately after evidence collection
  - Teardown immediately after collecting evidence: delete VM, OS disk, public
    IP, storage account/container, Managed Identity, NSG, VNet if demo-only,
    SSH key resource, and resource group if it only contains demo resources
  - Final cost check: inspect Azure All resources and Cost Management to
    confirm no demo resources remain

REQUIRED OUTPUT — update these files:

1. README.md
   - Add a "Roadmap" section with Day 1-5 above the existing 4-stage structure
   - Update "Project Structure" tree to include: dbt_project/, dags/ (Airflow),
     ai/ (LLM summary script)
   - Update tech stack table to add: dbt, Airflow 2.9, Gemini API, Azure
     (VM/Blob Storage/Managed Identity/Grafana Cloud)
   - Keep existing Stage 1-4 content intact — append, do not delete

2. AGENTS.md
   - Add new Hard Rules: 
     * "Airflow standalone mode only — no Celery executor (RAM constraint)"
     * "dbt models read from TimescaleDB, never bypass Spark sink"
     * "AI summary stage uses GOOGLE GEMINI API (gemini-2.5-flash) ONLY —
       never Claude API or OpenAI API — this stage must remain $0 cost"
     * "Day 5 uses Azure Free Account ($200 credit, 30 days): one Azure VM
       Standard_B2s for Kafka+Spark+PostgreSQL+Airflow, Blob Storage for
       Parquet, Managed Identity for storage access, NSG/VNet for network
       boundary"
     * "Before implementing any Azure step, confirm it fits the 30-day
       credit-backed demo plan; Standard_B2s uses credit and is NOT the
       B1s free VM. Flag any paid-tier or Pay-as-you-go requirement before
       writing code"
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
   - docs/stages/STAGE_8_AZURE_DEMO_RUNBOOK.md
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
- COST: The local roadmap must stay $0. AI layer uses Gemini API free
  tier (NOT Claude/OpenAI — those are pay-per-token with no meaningful
  free tier for sustained use). Azure Day 5 uses the $200/30-day credit
  for a short CV demo on one Standard_B2s VM, then tears down all resources.
- Do not invent file paths that don't match my actual project structure — 
  read the real files first
- Keep all existing content — this is an ADDITION, not a rewrite
- Vietnamese for explanations in docs/stages files, English for code 
  comments, consistent with existing files
- Do not start writing dbt/Airflow/Azure code yet — this task is
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
- **Azure Day 5 tối ưu chi phí**: dùng một `Standard_B2s` chạy full stack trong một phiên demo ngắn, dùng Azure Blob + Managed Identity, thu bằng chứng CV rồi teardown toàn bộ. Azure auto-shutdown và Cost Management chỉ là safety net/monitoring, không thay thế teardown.
- Lấy Gemini API key tại **aistudio.google.com/apikey** — hoàn toàn miễn phí, không cần khai báo thẻ.
