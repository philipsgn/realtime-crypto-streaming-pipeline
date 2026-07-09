#!/bin/bash
# Stage Day 3 — Airflow Orchestration
# File: infrastructure/entrypoint-airflow.sh
#
# Custom entrypoint that runs webserver in FOREGROUND mode.
# Fixes "No response from gunicorn master" on Docker Desktop/WSL2
# where daemon-mode fork consistently fails on resource-constrained machines.

set -e

echo "=== Airflow Custom Entrypoint ==="

# 1. Run database migrations
echo "Running database migrations..."
airflow db migrate

# 2. Create admin user (ignore if already exists)
echo "Creating admin user..."
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD:?_AIRFLOW_WWW_USER_PASSWORD is required}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com 2>/dev/null || echo "Admin user already exists"

# 3. Start scheduler in background
echo "Starting scheduler..."
airflow scheduler &

# 4. Start triggerer in background
echo "Starting triggerer..."
airflow triggerer &

# 5. Start webserver in FOREGROUND (not daemon mode)
# This is the key fix — avoids the daemon fork that fails on WSL2/Docker Desktop
echo "Starting webserver on port 8080 (foreground)..."
exec airflow webserver --port 8080
