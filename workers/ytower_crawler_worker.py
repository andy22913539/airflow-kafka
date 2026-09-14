import json
import os
import random
import signal
import sys
import time
from datetime import datetime
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer

from proxy_pool import (
    build_proxy_session,
    get_proxy_collection,
    lease_proxy,
    mark_proxy_failure,
    mark_proxy_success,
    release_proxy,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_JOB_TOPIC = os.getenv("KAFKA_JOB_TOPIC", "crawler_jobs")
KAFKA_RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "ytower_recipe_results")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ytower-crawler-group")
WORKER_NAME = os.getenv("WORKER_NAME", "crawler-worker")
NETWORK_MODE = os.getenv("CRAWLER_NETWORK_MODE", "proxy").strip().lower()

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "3"))
MAX_PROXY_SWITCHES_PER_SEQ = int(os.getenv("MAX_PROXY_SWITCHES_PER_SEQ", "5"))
PROXY_WAIT_SECONDS = int(os.getenv("PROXY_WAIT_SECONDS", "15"))
MAX_NOT_FOUND_LIMIT = int(os.getenv("MAX_NOT_FOUND_LIMIT", "50"))
COOLDOWN_SUCCESS_COUNT = int(os.getenv("COOLDOWN_SUCCESS_COUNT", "50"))
CRAWL_SLEEP_MIN = float(os.getenv("CRAWL_SLEEP_MIN", "2.5"))
CRAWL_SLEEP_MAX = float(os.getenv("CRAWL_SLEEP_MAX", "5.0"))
DIRECT_CRAWL_SLEEP_MIN = float(os.getenv("DIRECT_CRAWL_SLEEP_MIN", "4.0"))
DIRECT_CRAWL_SLEEP_MAX = float(os.getenv("DIRECT_CRAWL_SLEEP_MAX", "8.0"))
NOT_FOUND_SLEEP_MIN = float(os.getenv("NOT_FOUND_SLEEP_MIN", "0.5"))
NOT_FOUND_SLEEP_MAX = float(os.getenv("NOT_FOUND_SLEEP_MAX", "1.2"))
COOLDOWN_SLEEP_MIN = float(os.getenv("COOLDOWN_SLEEP_MIN", "10"))
COOLDOWN_SLEEP_MAX = float(os.getenv("COOLDOWN_SLEEP_MAX", "20"))
DIRECT_BLOCK_COOLDOWN_SECONDS = int(os.getenv("DIRECT_BLOCK_COOLDOWN_SECONDS", "900"))
DIRECT_BLOCK_MAX_RETRIES = int(os.getenv("DIRECT_BLOCK_MAX_RETRIES", "2"))
MAX_POLL_INTERVAL_MS = int(os.getenv("MAX_POLL_INTERVAL_MS", str(8 * 60 * 60 * 1000)))

CHALLENGE_MARKERS = (
    "captcha",
    "g-recaptcha",
    "hcaptcha",
    "cf-chl",
    "challenge-platform",
    "人機驗證",
    "驗證碼",
    "請點選",
    "請選取",
    "機器人驗證",
)

_stop_requested = False


class BlockedPageError(RuntimeError):
    pass


class RetryableRequestError(RuntimeError):
    pass


def _signal_handler(signum, frame):
    global _stop_requested
    _stop_requested = True
    print(f"[{WORKER_NAME}] received signal {signum}; stopping after current operation", flush=True)


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def candidate_seqs(prefix: str, seq_num: int):
    """同一個 numeric SEQ 同時保留網站的 4 位數與 3 位數格式。"""
    values = [f"{prefix}-{seq_num:04d}"]
    if seq_num < 1000:
        values.append(f"{prefix}-{seq_num:03d}")
    # 1000 以上兩種格式相同；小於 1000 時避免任何意外重複。
    return list(dict.fromkeys(values))


def build_direct_session() -> requests.Session:
    session = requests.Session()
    # Direct worker 明確不沿用主機或容器的 HTTP(S)_PROXY 環境變數。
    session.trust_env = False
    return session


def wait_for_proxy(proxy_collection):
    while not _stop_requested:
        proxy = lease_proxy(proxy_collection, WORKER_NAME)
        if proxy:
            session = build_proxy_session(proxy["url"])
            print(
                f"[{WORKER_NAME}] leased proxy={proxy['url']} exit={proxy.get('exit_ip')} "
                f"latency={proxy.get('latency_ms')}ms",
                flush=True,
            )
            return proxy, session
        print(
            f"[{WORKER_NAME}] no verified TW proxy available; staying out of Kafka group; "
            f"wait {PROXY_WAIT_SECONDS}s",
            flush=True,
        )
        time.sleep(PROXY_WAIT_SECONDS)
    raise RuntimeError("shutdown requested")


def looks_like_challenge(response: requests.Response) -> bool:
    if response.status_code in (403, 429):
        return True
    text = (response.text or "").lower()
    return any(marker.lower() in text for marker in CHALLENGE_MARKERS)


def request_recipe_page(
    seq: str, session: requests.Session
) -> Tuple[str, Optional[requests.Response], Optional[str], Optional[int]]:
    """
    回傳 (status, response, error, latency_ms)
      ok              -> HTTP 200 且未偵測到 challenge，交給 parser
      not_found       -> 明確 404/410
      blocked         -> CAPTCHA / challenge / 403 / 429；不能算 missing
      retryable_error -> timeout / connect / proxy / 5xx / 其他異常 HTTP；不能算 missing
    """
    url = f"https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq={seq}"
    last_error = None

    for attempt in range(1, REQUEST_RETRIES + 1):
        started = time.monotonic()
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            latency_ms = int((time.monotonic() - started) * 1000)

            if response.status_code in (404, 410):
                return "not_found", response, None, latency_ms

            if looks_like_challenge(response):
                return "blocked", response, f"challenge/http {response.status_code}", latency_ms

            if 500 <= response.status_code < 600:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)

            if response.status_code != 200:
                return (
                    "retryable_error",
                    response,
                    f"unexpected HTTP {response.status_code}",
                    latency_ms,
                )

            return "ok", response, None, latency_ms

        except (requests.Timeout, requests.ConnectionError, requests.ProxyError, requests.HTTPError) as exc:
            last_error = str(exc)
            if attempt < REQUEST_RETRIES:
                sleep_seconds = min(2 ** attempt, 8) + random.uniform(0, 1)
                print(
                    f"[{WORKER_NAME}] request retry {attempt}/{REQUEST_RETRIES} {seq}: "
                    f"{exc}; sleep={sleep_seconds:.1f}s",
                    flush=True,
                )
                time.sleep(sleep_seconds)

    return "retryable_error", None, last_error or "request failed", None


def parse_recipe_response(seq: str, seq_num: int, prefix: str, response: requests.Response) -> Optional[dict]:
    response.encoding = "big5"
    soup = BeautifulSoup(response.text, "html.parser")
    title_el = soup.select_one("#recipe_name h2 a")
    if not title_el or not title_el.text.strip():
        return None

    title = title_el.text.strip()
    time_el = soup.select_one("#recipe_info time")
    publish_date = (
        (time_el.get("datetime", "").strip() or time_el.text.strip()) if time_el else ""
    )
    keywords = [a.text.strip() for a in soup.select("div.recie_tag a") if a.text.strip()]

    ingredients = []
    for li in soup.select("#recipe_item ul.ingredient li"):
        name_a = li.select_one(".ingredient_name a")
        amount_span = li.select_one(".ingredient_amount")
        if name_a and amount_span:
            ingredients.append(f"{name_a.text.strip()} {amount_span.text.strip()}")
        elif name_a:
            ingredients.append(name_a.text.strip())

    steps = [li.text.strip() for li in soup.select("#recipe_info li.step") if li.text.strip()]

    return {
        "SEQ": seq,
        "seq_num": seq_num,
        "prefix": prefix,
        "食譜名稱": title,
        "上線日期": publish_date,
        "關鍵字": ", ".join(keywords),
        "食譜網址": f"https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq={seq}",
        "材料": " | ".join(ingredients),
        "做法步驟": "\n".join(steps),
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "crawler_worker": WORKER_NAME,
        "network_mode": NETWORK_MODE,
    }


def fetch_numeric_seq(
    prefix: str,
    seq_num: int,
    session: requests.Session,
    proxy_collection=None,
    proxy_doc=None,
):
    """
    一個 numeric SEQ 的 4 位數/3 位數候選要全部判斷完。
    只有兩種格式都確定沒有食譜時，才回 numeric_not_found。
    """
    for seq in candidate_seqs(prefix, seq_num):
        status, response, error, latency_ms = request_recipe_page(seq, session)

        if NETWORK_MODE == "proxy" and proxy_doc is not None and status in ("ok", "not_found"):
            mark_proxy_success(
                proxy_collection,
                proxy_doc["_id"],
                WORKER_NAME,
                latency_ms=latency_ms,
            )

        if status == "blocked":
            raise BlockedPageError(f"{seq}: {error or 'challenge detected'}")
        if status == "retryable_error":
            raise RetryableRequestError(f"{seq}: {error or 'request failed'}")
        if status == "not_found":
            continue

        if response is not None:
            try:
                data = parse_recipe_response(seq, seq_num, prefix, response)
            except Exception as exc:
                raise RetryableRequestError(f"parse error {seq}: {exc}") from exc
            if data is not None:
                return "found", data

            # YTower 對不存在的 SEQ 可能仍回 HTTP 200，但沒有 recipe DOM。
            # 已知 CAPTCHA/challenge 已在 request_recipe_page 先攔截，因此這裡視為候選格式不存在。
            continue

    return "numeric_not_found", None


def crawl_job(job: dict, producer: KafkaProducer, proxy_collection=None, initial_proxy=None, initial_session=None) -> int:
    prefix = str(job["prefix"])
    start_num = max(1, int(job.get("start_num", 1)))
    end_num = int(job.get("end_num", 5000))
    max_not_found = int(job.get("max_not_found_limit", MAX_NOT_FOUND_LIMIT))

    consecutive_not_found = 0
    success_count = 0
    produced = 0
    proxy_doc = initial_proxy
    session = initial_session

    if NETWORK_MODE == "direct":
        session = session or build_direct_session()
        print(
            f"[{WORKER_NAME}] start job prefix={prefix} range={start_num}-{end_num} mode=direct",
            flush=True,
        )
    else:
        if proxy_doc is None or session is None:
            proxy_doc, session = wait_for_proxy(proxy_collection)
        print(
            f"[{WORKER_NAME}] start job prefix={prefix} range={start_num}-{end_num} mode=proxy",
            flush=True,
        )

    try:
        for seq_num in range(start_num, end_num + 1):
            if _stop_requested:
                raise RuntimeError("shutdown requested")

            direct_block_retries = 0
            proxy_switches = 0

            while True:
                try:
                    numeric_status, data = fetch_numeric_seq(
                        prefix,
                        seq_num,
                        session,
                        proxy_collection=proxy_collection,
                        proxy_doc=proxy_doc,
                    )
                    break
                except BlockedPageError as exc:
                    if NETWORK_MODE == "direct":
                        direct_block_retries += 1
                        print(
                            f"[{WORKER_NAME}] BLOCKED/challenge at {prefix}-{seq_num}: {exc}; "
                            f"not counted as missing; cooldown {DIRECT_BLOCK_COOLDOWN_SECONDS}s "
                            f"({direct_block_retries}/{DIRECT_BLOCK_MAX_RETRIES})",
                            file=sys.stderr,
                            flush=True,
                        )
                        if direct_block_retries > DIRECT_BLOCK_MAX_RETRIES:
                            raise RuntimeError(
                                f"direct IP remains blocked at {prefix}-{seq_num}; job offset not committed"
                            ) from exc
                        time.sleep(DIRECT_BLOCK_COOLDOWN_SECONDS)
                        session.close()
                        session = build_direct_session()
                        continue

                    proxy_switches += 1
                    mark_proxy_failure(proxy_collection, proxy_doc["_id"], WORKER_NAME, str(exc))
                    session.close()
                    print(
                        f"[{WORKER_NAME}] proxy challenge at {prefix}-{seq_num}; switching proxy "
                        f"({proxy_switches}/{MAX_PROXY_SWITCHES_PER_SEQ})",
                        file=sys.stderr,
                        flush=True,
                    )
                    if proxy_switches >= MAX_PROXY_SWITCHES_PER_SEQ:
                        raise RuntimeError(
                            f"no usable proxy after {proxy_switches} challenge switches at {prefix}-{seq_num}"
                        ) from exc
                    proxy_doc, session = wait_for_proxy(proxy_collection)
                    continue

                except RetryableRequestError as exc:
                    if NETWORK_MODE == "direct":
                        raise RuntimeError(
                            f"direct retryable error at {prefix}-{seq_num}; job offset not committed: {exc}"
                        ) from exc

                    proxy_switches += 1
                    mark_proxy_failure(proxy_collection, proxy_doc["_id"], WORKER_NAME, str(exc))
                    session.close()
                    print(
                        f"[{WORKER_NAME}] proxy/network error at {prefix}-{seq_num}; switching proxy "
                        f"({proxy_switches}/{MAX_PROXY_SWITCHES_PER_SEQ})",
                        file=sys.stderr,
                        flush=True,
                    )
                    if proxy_switches >= MAX_PROXY_SWITCHES_PER_SEQ:
                        raise RuntimeError(
                            f"no usable proxy after {proxy_switches} switches at {prefix}-{seq_num}"
                        ) from exc
                    proxy_doc, session = wait_for_proxy(proxy_collection)
                    continue

            if numeric_status == "numeric_not_found":
                consecutive_not_found += 1
                if consecutive_not_found >= max_not_found:
                    print(
                        f"[{WORKER_NAME}] {prefix}: STOP at numeric seq={seq_num}; "
                        f"{consecutive_not_found} consecutive numeric SEQs missing",
                        flush=True,
                    )
                    break
                time.sleep(random.uniform(NOT_FOUND_SLEEP_MIN, NOT_FOUND_SLEEP_MAX))
                continue

            consecutive_not_found = 0
            success_count += 1
            if NETWORK_MODE == "proxy" and proxy_doc is not None:
                data["proxy_exit_ip"] = proxy_doc.get("exit_ip")
                data["proxy_country_code"] = proxy_doc.get("country_code")
            else:
                data["proxy_exit_ip"] = None
                data["proxy_country_code"] = None

            producer.send(
                KAFKA_RESULT_TOPIC,
                key=data["SEQ"].encode("utf-8"),
                value=data,
            ).get(timeout=30)
            produced += 1

            if NETWORK_MODE == "direct":
                time.sleep(random.uniform(DIRECT_CRAWL_SLEEP_MIN, DIRECT_CRAWL_SLEEP_MAX))
            else:
                time.sleep(random.uniform(CRAWL_SLEEP_MIN, CRAWL_SLEEP_MAX))

            if COOLDOWN_SUCCESS_COUNT > 0 and success_count % COOLDOWN_SUCCESS_COUNT == 0:
                time.sleep(random.uniform(COOLDOWN_SLEEP_MIN, COOLDOWN_SLEEP_MAX))

        producer.flush()
        print(f"[{WORKER_NAME}] finish {prefix}: produced={produced}", flush=True)
        return produced
    finally:
        if NETWORK_MODE == "proxy" and proxy_doc is not None:
            try:
                release_proxy(proxy_collection, proxy_doc["_id"], WORKER_NAME)
            except Exception:
                pass
        if session is not None:
            session.close()


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_JOB_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
        session_timeout_ms=30000,
        max_poll_records=1,
    )


def process_message(consumer, message, producer, proxy_collection=None, proxy_doc=None, session=None):
    crawl_job(
        message.value,
        producer,
        proxy_collection=proxy_collection,
        initial_proxy=proxy_doc,
        initial_session=session,
    )
    consumer.commit()


def main() -> int:
    if NETWORK_MODE not in {"direct", "proxy"}:
        raise ValueError("CRAWLER_NETWORK_MODE must be direct or proxy")

    mongo_client = None
    proxy_collection = None
    if NETWORK_MODE == "proxy":
        mongo_client, proxy_collection = get_proxy_collection()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
    )

    try:
        if NETWORK_MODE == "direct":
            consumer = create_consumer()
            print(
                f"[{WORKER_NAME}] mode=direct; always available on {KAFKA_JOB_TOPIC}",
                flush=True,
            )
            try:
                while not _stop_requested:
                    records = consumer.poll(timeout_ms=1000, max_records=1)
                    for _, messages in records.items():
                        for message in messages:
                            try:
                                process_message(consumer, message, producer)
                            except Exception as exc:
                                print(
                                    f"[{WORKER_NAME}] job failed; offset not committed: {exc}",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                time.sleep(5)
            finally:
                consumer.close()
        else:
            print(
                f"[{WORKER_NAME}] mode=proxy; leases a verified TW proxy before joining Kafka group",
                flush=True,
            )
            while not _stop_requested:
                proxy_doc = None
                session = None
                consumer = None
                try:
                    proxy_doc, session = wait_for_proxy(proxy_collection)
                    consumer = create_consumer()

                    # 只有拿到可用 Proxy 後才加入 consumer group 並取一個大 prefix job。
                    deadline = time.monotonic() + 15
                    message = None
                    while not _stop_requested and time.monotonic() < deadline and message is None:
                        records = consumer.poll(timeout_ms=1000, max_records=1)
                        for _, messages in records.items():
                            if messages:
                                message = messages[0]
                                break

                    if message is None:
                        continue

                    try:
                        process_message(
                            consumer,
                            message,
                            producer,
                            proxy_collection=proxy_collection,
                            proxy_doc=proxy_doc,
                            session=session,
                        )
                        # crawl_job 已關閉/釋放 initial proxy，避免 finally 重複處理。
                        proxy_doc = None
                        session = None
                    except Exception as exc:
                        print(
                            f"[{WORKER_NAME}] job failed; offset not committed: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                        time.sleep(5)
                finally:
                    if consumer is not None:
                        consumer.close()
                    if proxy_doc is not None:
                        try:
                            release_proxy(proxy_collection, proxy_doc["_id"], WORKER_NAME)
                        except Exception:
                            pass
                    if session is not None:
                        session.close()
    finally:
        producer.close()
        if mongo_client is not None:
            mongo_client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
