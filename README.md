# YTower Crawler Stack — V1 Full Run

這版保留 V1「每個 prefix 一個大 job」架構，改成 5 個 Kafka job partitions、1 個 Direct Worker + 4 個動態 TW Proxy Worker。**不讀 checkpoint、不依 MongoDB 既有進度決定起點；每次觸發 DAG 都從 numeric SEQ 1 完整重跑。**

## 主要服務

- Airflow 2.9.2：手動派發 90 個 prefix jobs
- Kafka：`crawler_jobs` 5 partitions；`ytower_recipe_results` 4 partitions
- `crawler-direct`：直接使用主機 / GCP 對外 IP
- `crawler-worker-1..4`：取得已驗證 TW Proxy 後才加入 Kafka consumer group
- `proxy-manager`：定期探索、驗證、更新 Proxy Pool
- `mongo-writer`：消費結果並 upsert 到 MongoDB
- MongoDB / MySQL / PostgreSQL

## 爬取規則

- Prefix：A01～I10，共 90 個。
- 每個 job：`start_num=1` 到 `MAX_SEQ_NUMBER=5000`（可由 `.env` 調整）。
- numeric SEQ 同時保留 4 位數 + 3 位數網站格式：7 → `0007` / `007`；100 → `0100` / `100`。
- 同一 numeric SEQ 的候選格式都沒有食譜才算 1 次 missing。
- 連續 `MAX_NOT_FOUND_LIMIT=50` 個 numeric SEQ 沒資料才停止該 prefix。
- CAPTCHA/challenge、403、429、網路/Proxy 錯誤不算 missing。
- Direct 遇 challenge：cooldown 後重試；仍被擋則該 Kafka job 不 commit。
- Proxy 遇 challenge：標記失敗並換 Proxy；不包含 CAPTCHA 自動解題。

## 第一次啟動

PowerShell：

```powershell
Copy-Item .env.example .env
notepad .env

docker compose build
docker compose up -d
docker compose ps
```

`.env` 不會放進 Git，請先從 `.env.example` 建立並填入密碼與 Airflow Fernet Key。

## 開始完整重跑

```powershell
$Date = Get-Date -Format "yyyy-MM-dd"

docker compose exec airflow-scheduler `
  airflow dags test `
  ytower_recipe_dispatch_full_run `
  $Date
```

每執行一次都會重新派發 90 個從 1 開始的大 job，請不要在同一輪尚未完成時重複觸發。

## 檢查

```powershell
docker compose logs -f crawler-direct
```

```powershell
docker compose logs -f proxy-manager
```

```powershell
docker compose logs -f mongo-writer
```

Kafka job topic 應為 5 partitions：

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --describe `
  --topic crawler_jobs
```

更完整說明請看 `FULL_RUN_V1.md`。
