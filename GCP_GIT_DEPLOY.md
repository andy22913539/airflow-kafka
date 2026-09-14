# GCP Git Clone Deployment

## 1. Clone

```bash
git clone <YOUR_REPOSITORY_URL>
cd recipe_stack_FINAL_GIT_PUSH
```

## 2. Create the real environment file

`.env` is intentionally excluded from Git.

```bash
cp .env.example .env
nano .env
chmod 600 .env
```

Replace every `CHANGE_ME_...` value before first startup.

Generate secrets, for example:

```bash
openssl rand -hex 32
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Use the Fernet output only for `AIRFLOW_FERNET_KEY`.

## 3. Validate Compose

```bash
docker compose config --services
```

## 4. First startup

```bash
docker compose build
docker compose up -d
docker compose ps
```

## 5. Check core services

```bash
docker compose logs --tail=100 proxy-manager
docker compose logs --tail=100 crawler-worker-1
docker compose logs --tail=100 mongo-writer
```

## Important

- Do not commit `.env`.
- Docker-internal addresses stay `kafka:9092`, `mongodb:27017`, `mysql:3306`, and `postgres:5432`.
- Host ports such as `9094`, `27018`, and `3307` are only for access from outside the Compose network.
- Runtime folders such as database data and Airflow logs are intentionally not stored in Git; Docker/Compose creates them on the GCP VM.
- Changing an initialization password in `.env` does not automatically change the password inside an already initialized persistent database. For a new GCP deployment, set final passwords before the first `docker compose up`.
