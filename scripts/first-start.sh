#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "[1/3] Creating .env with generated secrets..."
  bash scripts/bootstrap-env.sh
else
  echo "[1/3] .env already exists; keeping it."
fi

echo "[2/3] Building and starting the stack..."
docker compose --env-file .env up -d --build

echo "[3/3] Current service status:"
docker compose --env-file .env ps

echo
echo "Done. Runtime data is stored in Docker named volumes; Git-managed DAG/source files are never chowned by containers."
