Day 1 — Observability UIs added

Goals completed:
- Add Kafdrop (Kafka UI) to Docker Compose, configured to use host networking and connect to localhost:9092
- Add pgAdmin (Postgres UI) to Docker Compose for TimescaleDB inspection
- Update README.md quickstart with Kafdrop and pgAdmin URLs
- Ensure RAM budget: +200MB for observability services (kafdrop 200m, pgadmin 200m)

Why: Visual inspection of topics/messages and database contents accelerates debugging and verification during Stage 1-3.

How to run:
1. Start stack: `docker compose -f infrastructure/docker-compose.yml up -d`
2. Start producer: `make producer` (or `python ingestion/binance_producer.py`)
3. Start processing: `make spark-job` (or `python processing/spark_streaming.py`)
4. Open UIs:
   - Grafana: http://localhost:3000  (admin/admin)
   - Kafdrop: http://localhost:9000
   - pgAdmin: http://localhost:5050  (admin@crypto.com / admin)

Notes:
- Kafdrop uses host network to reach `localhost:9092` from Windows host.
- Keep total RAM impact within project limits as recorded in AGENTS.md.
