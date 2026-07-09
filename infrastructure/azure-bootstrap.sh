#!/usr/bin/env bash
set -euo pipefail

PROJECT_REPO="${PROJECT_REPO:-https://github.com/philipsgn/realtime-crypto-streaming-pipeline.git}"
PROJECT_DIR="${PROJECT_DIR:-$HOME/realtime-crypto-streaming-pipeline}"

sudo apt-get update -q
sudo apt-get install -y ca-certificates curl git openjdk-17-jdk python3.10-venv python3-pip

if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi

sudo usermod -aG docker "$USER"

if [ ! -d "$PROJECT_DIR/.git" ]; then
  git clone "$PROJECT_REPO" "$PROJECT_DIR"
else
  git -C "$PROJECT_DIR" pull --ff-only origin main
fi

cd "$PROJECT_DIR"

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if [ ! -f .env ]; then
  cp .env.example .env
  chmod 600 .env
fi

if [ ! -f .env.docker ]; then
  {
    echo "KAFKA_BOOTSTRAP_SERVERS=kafka:9093"
    echo "KAFKA_TOPIC=crypto-trades"
    echo "SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT"
    echo "POSTGRES_HOST=postgres"
    echo "POSTGRES_PORT=5432"
    echo "POSTGRES_DB=crypto_pipeline"
    echo "POSTGRES_USER=pipeline"
    echo "# Change this before starting services"
    echo "POSTGRES_PASSWORD=CHANGE_ME"
    echo "GRAFANA_ADMIN_PASSWORD=CHANGE_ME"
    echo "GF_DATASOURCE_POSTGRES_PASSWORD=CHANGE_ME"
    echo "PGADMIN_DEFAULT_PASSWORD=CHANGE_ME"
    echo "AIRFLOW_ADMIN_PASSWORD=CHANGE_ME"
    echo "PARQUET_OUTPUT=/tmp/crypto_raw"
    echo "CHECKPOINT_DIR=/tmp/checkpoint"
    echo "RESET_SPARK_STATE=false"
    echo "AZURE_STORAGE_ACCOUNT="
    echo "GEMINI_API_KEY="
  } > .env.docker
  chmod 600 .env.docker
fi

echo "Bootstrap complete."
echo "Next: edit $PROJECT_DIR/.env and $PROJECT_DIR/.env.docker with real"
echo "POSTGRES_PASSWORD, GEMINI_API_KEY, PARQUET_OUTPUT and AZURE_STORAGE_ACCOUNT."
echo "Log out and SSH again if Docker group permission is not active yet."
