# Changes and validation

## 主要修改

- 移除固定 `PROXY_1..PROXY_4`。
- 新增 `proxy-manager` 常駐服務。
- 免費來源以 Taiwan/TW 為主：ProxyScrape、Databay、IPLocate。
- Proxy 經 HTTPS 實際 request 驗證，不只相信來源清單。
- 驗證出口 IP，並再次確認 country code = TW。
- Proxy 資料保存於 MongoDB `proxy_pool`。
- 四個 crawler worker 透過 lease 取得不同可用 Proxy。
- Proxy timeout/連線錯誤/429/5xx 會切換代理，不算成「食譜不存在」。
- 爬蟲結果仍然先送 Kafka，再由 `mongo-writer` upsert 到 MongoDB。
- Airflow DAG 更名為 `ytower_recipe_dispatch_proxy_pool`。

## 已做靜態驗證

- Python `compileall` 通過。
- `docker-compose.yml` YAML parsing 通過。
- Compose 中存在 proxy-manager、4 crawler workers、mongo-writer。

## GCP VM 上仍需做的 runtime 驗證

```bash
docker compose build
docker compose up -d
docker compose ps
docker compose logs proxy-manager
./scripts/verify-stack.sh
```

因免費 TW Proxy 供應量會變動，`proxy-manager` 顯示 0 個可用 Proxy 不一定代表程式錯誤，需同時檢查各來源 log 與當下公開 Proxy 供應。
