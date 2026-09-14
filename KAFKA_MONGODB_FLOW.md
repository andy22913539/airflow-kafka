# Kafka -> MongoDB 與 Dynamic Proxy Pool 流程

```text
Airflow DAG
  -> Kafka crawler_jobs (4 partitions)
     -> crawler-worker-1..4
        -> MongoDB proxy_pool lease
        -> verified TW proxy
        -> YTower
        -> Kafka ytower_recipe_results
           -> mongo-writer
              -> MongoDB recipes (SEQ unique + upsert)
```

Crawler 不直接寫 recipes collection。完整食譜先送進 Kafka result topic，只有 `mongo-writer` 負責 MongoDB recipes 寫入。

Proxy 本身則由 `proxy-manager` 寫入 `proxy_pool` collection；crawler 只租用已被標記 `is_alive=true` 且 `country_code=TW` 的代理。

網路/Proxy timeout、429、5xx 不會被算進 `MAX_NOT_FOUND_LIMIT`。Proxy 失效時會更換 Proxy 並重新嘗試同一個 SEQ；若單一 SEQ 已切換太多代理仍失敗，整個 Kafka prefix job 不 commit，之後可重新投遞。MongoDB recipes 使用 SEQ unique + upsert，因此重跑具備冪等性。
