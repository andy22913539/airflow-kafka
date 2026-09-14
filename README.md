# Recipe Stack - Kafka + MongoDB + Dynamic Taiwan Proxy Pool

這版保留原本資料流：

```text
Airflow -> Kafka(crawler_jobs) -> 4 crawler workers -> Dynamic TW Proxy Pool -> YTower
                                                -> Kafka(ytower_recipe_results)
                                                -> mongo-writer -> MongoDB
```

## Proxy 不再手動維護

`proxy-manager` 會定期從機器可讀的免費來源收集台灣 Proxy，驗證 HTTPS、出口 IP、國家代碼與延遲，存入 MongoDB `recipe_ai.proxy_pool`。

目前預設來源：

1. ProxyScrape v4 Taiwan free proxy API
   - https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&country=tw
2. Databay country-specific Taiwan proxy lists
   - https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list/by-country/tw/http.txt
   - https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list/by-country/tw/socks4.txt
   - https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list/by-country/tw/socks5.txt
3. IPLocate Taiwan proxy list
   - https://raw.githubusercontent.com/iplocate/free-proxy-list/main/countries/TW/proxies.txt

Proxy 實際出口 IP 會先透過 `api.ipify.org` 驗證，再使用 `countries.dev`（失敗時 fallback 到 `ipwho.is`）確認出口國家為 `TW`。

> 免費 Proxy 數量與可用性波動很大；如果當下沒有通過驗證的 TW Proxy，crawler worker 會等待，不會偷偷使用 GCP 真實出口 IP。

## 啟動

```bash
./scripts/bootstrap-env.sh
docker compose build
docker compose up -d
```

查看 Proxy Pool：

```bash
docker compose logs -f proxy-manager
docker compose exec -T proxy-manager python /app/scripts/check_proxy_pool.py
```

完整驗證：

```bash
./scripts/verify-stack.sh
```

確認有可用 TW Proxy 後，觸發 Airflow DAG：

```text
ytower_recipe_dispatch_proxy_pool
```

## MongoDB Collections

- `recipes`：由 `mongo-writer` 從 Kafka `ytower_recipe_results` upsert。
- `proxy_pool`：由 `proxy-manager` 維護免費代理的來源、出口 IP、TW 國家驗證、延遲、成功失敗次數與 lease。

## Proxy Pool 設定

`.env` 主要參數：

```env
PROXY_COUNTRY_CODE=TW
PROXY_DISCOVERY_INTERVAL=1800
PROXY_VALIDATION_TIMEOUT=8
PROXY_VALIDATION_WORKERS=20
PROXY_MAX_VALIDATE_PER_CYCLE=120
PROXY_MAX_LATENCY_MS=5000
PROXY_MAX_FAILURES=3
PROXY_LEASE_SECONDS=1800
PROXY_WAIT_SECONDS=15
MAX_PROXY_SWITCHES_PER_SEQ=5
PROXY_STALE_HOURS=12
```

四個 crawler worker 會以 MongoDB 原子 lease 方式取得不同 Proxy。Proxy 失敗會增加失敗次數、釋放租約並換下一個；連續失敗達門檻會暫停使用。Prefix 完成或 worker 離開時會釋放 Proxy。

## 安全注意

免費公開 Proxy 不可信。這個專案只應讓公開網站 GET 請求經過免費 Proxy，不要傳送密碼、Cookie、API token 或其他敏感資料。程式沒有關閉 TLS certificate verification。
