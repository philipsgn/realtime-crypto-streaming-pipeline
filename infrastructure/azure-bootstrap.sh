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

echo "Bootstrap complete."
echo "Next: edit $PROJECT_DIR/.env with real POSTGRES_PASSWORD, GEMINI_API_KEY,"
echo "PARQUET_OUTPUT=wasbs://raw-trades@<account>.blob.core.windows.net/ and"
echo "AZURE_STORAGE_ACCOUNT=<account>."
echo "Log out and SSH again if Docker group permission is not active yet."
