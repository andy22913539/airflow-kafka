# V1 Full Run（5 partitions / Direct + 4 Proxy Workers）

這個版本**不讀取 checkpoint，也不讀 MongoDB 最新 SEQ 來決定起點**。每次手動觸發 DAG `ytower_recipe_dispatch_full_run` 時，都會為 A01～I10 共 90 個 prefix 派發一個大 job，範圍固定從 numeric SEQ 1 到 `MAX_SEQ_NUMBER`（預設 5000）。

## 執行架構

- Kafka `crawler_jobs`: 5 partitions
- `crawler-direct`: 使用主機/GCP 的直接出口 IP
- `crawler-worker-1` ～ `crawler-worker-4`: 只有取得已驗證 TW Proxy 後才加入 Kafka consumer group
- 5 個 crawler 使用同一個 consumer group，由 Kafka 分配 90 個 prefix jobs
- 結果送到 `ytower_recipe_results`，再由 `mongo-writer` upsert 到 MongoDB

## SEQ 與 50 misses

同一 numeric SEQ 會依序檢查網站的 4 位數與 3 位數格式。例如 numeric 7 會檢查 `0007`、`007`；numeric 100 會檢查 `0100`、`100`。只有同一 numeric SEQ 的候選格式全部沒有食譜，才把 consecutive missing 加 1。找到食譜會歸零；連續 50 個 numeric SEQ 都沒有資料才停止該 prefix。

## CAPTCHA / challenge

- 403、429，以及常見 CAPTCHA/challenge HTML marker 不算 missing。
- Direct worker：偵測到 challenge 後 cooldown，重試仍被擋則不 commit 該 Kafka job，避免誤判 50 misses。
- Proxy worker：偵測到 challenge 後標記該 Proxy failure、切換下一個 Proxy；Proxy 不足時不把 job 當完成。
- 不包含 CAPTCHA 自動破解或圖片解題。

## 第一次啟動

```powershell
Copy-Item .env.example .env
notepad .env
docker compose build
docker compose up -d
docker compose ps
```

新資料夾第一次啟動時 bind-mount data 目錄會是新的，因此 Mongo/Kafka 也是乾淨環境。若沿用舊資料夾，請先自行備份再決定是否清除 `mongodb/data`、`kafka/data` 等持久化資料。

## 手動開始完整重跑

```powershell
$Date = Get-Date -Format "yyyy-MM-dd"
docker compose exec airflow-scheduler `
  airflow dags test `
  ytower_recipe_dispatch_full_run `
  $Date
```

不要重複觸發；每次觸發都會重新送出 90 個從 1 開始的大 job。

## 驗證 partitions

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --describe `
  --topic crawler_jobs
```

應看到 `PartitionCount: 5`。
