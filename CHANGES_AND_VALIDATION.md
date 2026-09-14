# V1 Full Run 修改摘要

- 回到 V1：每個 prefix 一個大 job，不切 250 chunk。
- Kafka `crawler_jobs` 改為 5 partitions，並在既有 topic 少於 5 partitions 時自動增加。
- 新增 `crawler-direct`，使用直接出口 IP。
- 保留 4 個 Proxy workers；Proxy worker 取得有效 TW Proxy 後才加入 Kafka consumer group。
- Airflow DAG 改為 `ytower_recipe_dispatch_full_run`，完全忽略 checkpoint 與 MongoDB 既有最大 SEQ；每次固定從 1 開始。
- 3 位數 / 4 位數候選以 numeric SEQ 為單位判斷，避免一個 numeric SEQ 被計成兩次 missing。
- CAPTCHA/challenge、403、429、網路/Proxy 錯誤不計入 consecutive missing。
- Direct worker 遇 challenge 會 cooldown；Proxy worker 遇 challenge 會切換 Proxy。
- 靜態 Python compile 與 docker-compose YAML parse 已通過。
