#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== docker compose services =="
docker compose ps

echo
echo "== Kafka topics =="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

echo
echo "== Dynamic Taiwan proxy pool =="
docker compose exec -T proxy-manager python /app/scripts/check_proxy_pool.py || {
  echo "No verified TW proxy is available yet. Check: docker compose logs proxy-manager"
  exit 2
}

echo
echo "== Airflow connection DAG =="
docker compose exec -T airflow-scheduler airflow dags test test_connections_pipeline "$(date +%F)"
