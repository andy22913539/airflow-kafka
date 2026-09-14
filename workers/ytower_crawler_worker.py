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

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "3"))
MAX_PROXY_SWITCHES_PER_SEQ = int(os.getenv("MAX_PROXY_SWITCHES_PER_SEQ", "5"))
PROXY_WAIT_SECONDS = int(os.getenv("PROXY_WAIT_SECONDS", "15"))
MAX_NOT_FOUND_LIMIT = int(os.getenv("MAX_NOT_FOUND_LIMIT", "50"))
COOLDOWN_SUCCESS_COUNT = int(os.getenv("COOLDOWN_SUCCESS_COUNT", "50"))
CRAWL_SLEEP_MIN = float(os.getenv("CRAWL_SLEEP_MIN", "2.5"))
CRAWL_SLEEP_MAX = float(os.getenv("CRAWL_SLEEP_MAX", "5.0"))
NOT_FOUND_SLEEP_MIN = float(os.getenv("NOT_FOUND_SLEEP_MIN", "0.5"))
NOT_FOUND_SLEEP_MAX = float(os.getenv("NOT_FOUND_SLEEP_MAX", "1.2"))
COOLDOWN_SLEEP_MIN = float(os.getenv("COOLDOWN_SLEEP_MIN", "10"))
COOLDOWN_SLEEP_MAX = float(os.getenv("COOLDOWN_SLEEP_MAX", "20"))
MAX_POLL_INTERVAL_MS = int(os.getenv("MAX_POLL_INTERVAL_MS", str(8 * 60 * 60 * 1000)))

_stop_requested = False


def _signal_handler(signum, frame):
    global _stop_requested
    _stop_requested = True
    print(f"[{WORKER_NAME}] received signal {signum}; stopping after current operation", flush=True)


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def generate_seq_for_prefix(prefix: str, start_num: int, end_num: int):
    """保留 YTower 4 位數 + 3 位數雙格式 SEQ。"""
    for n2 in range(start_num, end_num + 1):
        yield n2, f"{prefix}-{n2:04d}"
        if n2 < 1000:
            yield n2, f"{prefix}-{n2:03d}"


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
            f"[{WORKER_NAME}] no verified TW proxy available; wait {PROXY_WAIT_SECONDS}s",
            flush=True,
        )
        time.sleep(PROXY_WAIT_SECONDS)
    raise RuntimeError("shutdown requested")


def request_recipe_page(seq: str, session: requests.Session) -> Tuple[str, Optional[requests.Response], Optional[str], Optional[int]]:
    """
    回傳 (status, response, error, latency_ms)
      found       -> HTTP 200，交給 parser
      not_found   -> 404/410 或合法回應但沒有食譜內容
      proxy_error -> timeout / connect / proxy / 429 / 5xx，應更換 proxy，不能算 missing
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

            if response.status_code == 429 or 500 <= response.status_code < 600:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)

            if response.status_code != 200:
                return "not_found", response, None, latency_ms

            return "found", response, None, latency_ms

        except (requests.Timeout, requests.ConnectionError, requests.ProxyError, requests.HTTPError) as exc:
            last_error = str(exc)
            if attempt < REQUEST_RETRIES:
                sleep_seconds = min(2 ** attempt, 8) + random.uniform(0, 1)
                print(
                    f"[{WORKER_NAME}] proxy request retry {attempt}/{REQUEST_RETRIES} {seq}: "
                    f"{exc}; sleep={sleep_seconds:.1f}s",
                    flush=True,
                )
                time.sleep(sleep_seconds)

    return "proxy_error", None, last_error or "proxy request failed", None


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
    }


def crawl_job(job: dict, producer: KafkaProducer, proxy_collection) -> int:
    """Kafka job -> 動態 TW proxy -> YTower -> Kafka result。MongoDB 寫入仍由 mongo-writer 負責。"""
    prefix = str(job["prefix"])
    start_num = max(1, int(job.get("start_num", 1)))
    end_num = int(job.get("end_num", 5000))
    max_not_found = int(job.get("max_not_found_limit", MAX_NOT_FOUND_LIMIT))

    consecutive_not_found = 0
    success_count = 0
    produced = 0
    proxy_doc, session = wait_for_proxy(proxy_collection)

    print(f"[{WORKER_NAME}] start job prefix={prefix} range={start_num}-{end_num}", flush=True)

    try:
        for seq_num, seq in generate_seq_for_prefix(prefix, start_num, end_num):
            if _stop_requested:
                raise RuntimeError("shutdown requested")

            switches = 0
            while True:
                status, response, error, latency_ms = request_recipe_page(seq, session)

                if status != "proxy_error":
                    mark_proxy_success(
                        proxy_collection,
                        proxy_doc["_id"],
                        WORKER_NAME,
                        latency_ms=latency_ms,
                    )
                    break

                switches += 1
                mark_proxy_failure(proxy_collection, proxy_doc["_id"], WORKER_NAME, error or "unknown")
                session.close()
                print(
                    f"[{WORKER_NAME}] proxy failed for {seq}; switching proxy "
                    f"({switches}/{MAX_PROXY_SWITCHES_PER_SEQ})",
                    file=sys.stderr,
                    flush=True,
                )

                if switches >= MAX_PROXY_SWITCHES_PER_SEQ:
                    # 不 commit 這個 prefix job；稍後 Kafka 會重新投遞，Mongo upsert 可處理重複結果。
                    raise RuntimeError(f"no usable proxy for {seq} after {switches} switches")

                proxy_doc, session = wait_for_proxy(proxy_collection)

            data = None
            if status == "found" and response is not None:
                try:
                    data = parse_recipe_response(seq, seq_num, prefix, response)
                except Exception as exc:
                    print(f"[{WORKER_NAME}] parse error {seq}: {exc}", flush=True)

            if data is None:
                consecutive_not_found += 1
                if consecutive_not_found >= max_not_found:
                    print(
                        f"[{WORKER_NAME}] {prefix}: stop after {consecutive_not_found} consecutive misses",
                        flush=True,
                    )
                    break
                time.sleep(random.uniform(NOT_FOUND_SLEEP_MIN, NOT_FOUND_SLEEP_MAX))
                continue

            consecutive_not_found = 0
            success_count += 1
            data["proxy_exit_ip"] = proxy_doc.get("exit_ip")
            data["proxy_country_code"] = proxy_doc.get("country_code")

            producer.send(
                KAFKA_RESULT_TOPIC,
                key=seq.encode("utf-8"),
                value=data,
            ).get(timeout=30)
            produced += 1

            time.sleep(random.uniform(CRAWL_SLEEP_MIN, CRAWL_SLEEP_MAX))
            if COOLDOWN_SUCCESS_COUNT > 0 and success_count % COOLDOWN_SUCCESS_COUNT == 0:
                time.sleep(random.uniform(COOLDOWN_SLEEP_MIN, COOLDOWN_SLEEP_MAX))

        producer.flush()
        print(f"[{WORKER_NAME}] finish {prefix}: produced={produced}", flush=True)
        return produced
    finally:
        try:
            release_proxy(proxy_collection, proxy_doc["_id"], WORKER_NAME)
        except Exception:
            pass
        session.close()


def main() -> int:
    mongo_client, proxy_collection = get_proxy_collection()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
    )
    consumer = KafkaConsumer(
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

    print(f"[{WORKER_NAME}] waiting for jobs on {KAFKA_JOB_TOPIC}; proxy_pool=MongoDB", flush=True)
    try:
        while not _stop_requested:
            records = consumer.poll(timeout_ms=1000, max_records=1)
            for _, messages in records.items():
                for message in messages:
                    try:
                        crawl_job(message.value, producer, proxy_collection)
                        consumer.commit()
                    except Exception as exc:
                        print(
                            f"[{WORKER_NAME}] job failed; offset not committed: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                        time.sleep(5)
    finally:
        consumer.close()
        producer.close()
        mongo_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
