# Permission-safe deployment

This version deliberately separates **Git source code** from **container runtime data**.

## Why Git pulls no longer hit UID 50000 / root ownership

- `./airflow/dags` is mounted **read-only** into Airflow.
- `init-permissions` never mounts or `chown`s `./airflow/dags`.
- Airflow logs/plugins use Docker named volumes.
- MySQL, MongoDB, Kafka and PostgreSQL data use Docker named volumes.
- Therefore database containers do not create/chown `mysql/data`, `mongodb/data`, `kafka/data`, or `postgres/data` inside the Git checkout.

## First deployment on a new GCP VM

```bash
git clone https://github.com/andy22913539/airflow-kafka.git
cd airflow-kafka
bash scripts/first-start.sh
```

`first-start.sh` creates `.env` with generated secrets when it does not exist, then builds and starts the stack.

## Normal updates

```bash
git pull origin main
docker compose --env-file .env up -d --build
```

No `sudo chown -R ... airflow/dags` should be needed.

## Persistent data

Docker named volumes are intentionally persistent across `docker compose down` and Git updates.
To list them:

```bash
docker volume ls
```

`docker compose down -v` deletes the project's named volumes and therefore deletes DB/Kafka runtime data. Do not use `-v` unless a full data reset is intended.
