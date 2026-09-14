import ipaddress
import os
import re
import signal
import socket
import sys
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from urllib.parse import urlparse

import requests

from pymongo import UpdateOne

from proxy_pool import get_proxy_collection, utcnow


# ============================================================
# Environment
# ============================================================

PROXY_COUNTRY_CODE = os.getenv(
    "PROXY_COUNTRY_CODE",
    "TW",
).upper()

DISCOVERY_INTERVAL = int(
    os.getenv(
        "PROXY_DISCOVERY_INTERVAL",
        "1800",
    )
)

VALIDATION_TIMEOUT = float(
    os.getenv(
        "PROXY_VALIDATION_TIMEOUT",
        "15",
    )
)

VALIDATION_WORKERS = int(
    os.getenv(
        "PROXY_VALIDATION_WORKERS",
        "20",
    )
)

MAX_VALIDATE_PER_CYCLE = int(
    os.getenv(
        "PROXY_MAX_VALIDATE_PER_CYCLE",
        "200",
    )
)

MAX_LATENCY_MS = int(
    os.getenv(
        "PROXY_MAX_LATENCY_MS",
        "10000",
    )
)

TCP_TIMEOUT = float(
    os.getenv(
        "PROXY_TCP_TIMEOUT",
        "3",
    )
)

STALE_HOURS = int(
    os.getenv(
        "PROXY_STALE_HOURS",
        "12",
    )
)


IP_PORT_RE = re.compile(
    r"^(?P<host>\d{1,3}(?:\.\d{1,3}){3}):(?P<port>\d{1,5})$"
)

_stop = False


# ============================================================
# Sources
# ============================================================

TEXT_SOURCES = [
    {
        "name": "proxyscrape_tw",
        "url": (
            "https://api.proxyscrape.com/v4/free-proxy-list/get"
            "?request=display_proxies"
            "&proxy_format=protocolipport"
            "&format=text"
            "&country=tw"
        ),
        "scheme": None,
    },
    {
        "name": "databay_tw_http",
        "url": (
            "https://cdn.jsdelivr.net/gh/"
            "databay-labs/free-proxy-list/"
            "by-country/tw/http.txt"
        ),
        "scheme": "http",
    },
    {
        "name": "databay_tw_socks4",
        "url": (
            "https://cdn.jsdelivr.net/gh/"
            "databay-labs/free-proxy-list/"
            "by-country/tw/socks4.txt"
        ),
        "scheme": "socks4",
    },
    {
        "name": "databay_tw_socks5",
        "url": (
            "https://cdn.jsdelivr.net/gh/"
            "databay-labs/free-proxy-list/"
            "by-country/tw/socks5.txt"
        ),
        "scheme": "socks5h",
    },
]


JSON_SOURCES = [
    {
        "name": "proxio_all",
        "url": (
            "https://raw.githubusercontent.com/"
            "proxio-io/proxy-list/main/all.json"
        ),
        "type": "proxio",
    },
    {
        "name": "proxyscrape_github",
        "url": (
            "https://cdn.jsdelivr.net/gh/"
            "proxyscrape/free-proxy-list@main/"
            "proxies/all/data.json"
        ),
        "type": "proxyscrape",
    },
]


# ============================================================
# Signal
# ============================================================

def on_signal(signum, frame):
    global _stop

    _stop = True

    print(
        f"[proxy-manager] received signal={signum}; stopping...",
        flush=True,
    )


signal.signal(
    signal.SIGTERM,
    on_signal,
)

signal.signal(
    signal.SIGINT,
    on_signal,
)


# ============================================================
# Normalize
# ============================================================

def normalize_proxy(raw: str, default_scheme=None):
    if not raw:
        return None

    value = str(raw).strip().strip("'").strip('"')

    if not value:
        return None

    if value.startswith("#"):
        return None

    if "://" not in value:
        if not default_scheme:
            return None

        if not IP_PORT_RE.match(value):
            return None

        value = f"{default_scheme}://{value}"

    try:
        parsed = urlparse(value)
    except Exception:
        return None

    scheme = parsed.scheme.lower()

    if scheme == "socks5":
        scheme = "socks5h"

    if scheme not in {
        "http",
        "https",
        "socks4",
        "socks5h",
    }:
        return None

    if not parsed.hostname:
        return None

    try:
        port = parsed.port
    except ValueError:
        return None

    if not port:
        return None

    try:
        ipaddress.ip_address(
            parsed.hostname
        )
    except ValueError:
        return None

    if not 1 <= port <= 65535:
        return None

    return (
        f"{scheme}://"
        f"{parsed.hostname}:"
        f"{port}"
    )


# ============================================================
# Text sources
# ============================================================

def fetch_text_source(source):
    try:
        response = requests.get(
            source["url"],
            timeout=20,
            headers={
                "User-Agent": "recipe-proxy-manager/2.0",
            },
        )

        if response.status_code == 404:
            print(
                f"[proxy-manager] "
                f"source={source['name']} returned 404",
                flush=True,
            )
            return []

        response.raise_for_status()

    except Exception as exc:
        print(
            f"[proxy-manager] "
            f"source failed "
            f"{source['name']}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return []

    results = []

    for line in response.text.splitlines():
        proxy_url = normalize_proxy(
            line,
            source.get("scheme"),
        )

        if proxy_url:
            results.append(proxy_url)

    print(
        f"[proxy-manager] "
        f"source={source['name']} "
        f"candidates={len(results)}",
        flush=True,
    )

    return results


# ============================================================
# JSON sources
# ============================================================

def protocols_to_proxy_urls(ip, port, protocols):
    results = []

    if not ip or not port:
        return results

    try:
        ipaddress.ip_address(str(ip))
        port = int(port)
    except (ValueError, TypeError):
        return results

    if isinstance(protocols, str):
        protocols = [protocols]

    if not isinstance(protocols, list):
        return results

    for protocol in protocols:
        protocol = str(protocol).lower().strip()

        if protocol == "socks5":
            protocol = "socks5h"

        if protocol not in {
            "http",
            "https",
            "socks4",
            "socks5h",
        }:
            continue

        proxy_url = normalize_proxy(
            f"{protocol}://{ip}:{port}"
        )

        if proxy_url:
            results.append(proxy_url)

    return results


def parse_proxio_json(data):
    results = []

    if isinstance(data, dict):
        rows = (
            data.get("proxies")
            or data.get("data")
            or []
        )
    else:
        rows = data

    if not isinstance(rows, list):
        return results

    for item in rows:
        if not isinstance(item, dict):
            continue

        country = str(
            item.get("country_code")
            or item.get("countryCode")
            or item.get("country")
            or ""
        ).upper()

        # 支援 TW 或 Taiwan
        if country not in {
            "TW",
            "TAIWAN",
        }:
            continue

        ip = (
            item.get("ip")
            or item.get("host")
        )

        port = item.get("port")

        protocols = (
            item.get("protocols")
            or item.get("protocol")
            or item.get("type")
            or []
        )

        results.extend(
            protocols_to_proxy_urls(
                ip,
                port,
                protocols,
            )
        )

    return results


def parse_proxyscrape_json(data):
    results = []

    if isinstance(data, dict):
        rows = (
            data.get("proxies")
            or data.get("data")
            or data.get("results")
            or []
        )
    else:
        rows = data

    if not isinstance(rows, list):
        return results

    for item in rows:
        if not isinstance(item, dict):
            continue

        country = str(
            item.get("country_code")
            or item.get("countryCode")
            or item.get("country")
            or ""
        ).upper()

        if country not in {
            "TW",
            "TAIWAN",
        }:
            continue

        ip = (
            item.get("ip")
            or item.get("host")
        )

        port = item.get("port")

        protocols = (
            item.get("protocol")
            or item.get("protocols")
            or item.get("type")
            or []
        )

        results.extend(
            protocols_to_proxy_urls(
                ip,
                port,
                protocols,
            )
        )

    return results


def fetch_json_source(source):
    try:
        response = requests.get(
            source["url"],
            timeout=30,
            headers={
                "User-Agent": "recipe-proxy-manager/2.0",
            },
        )

        if response.status_code == 404:
            print(
                f"[proxy-manager] "
                f"source={source['name']} returned 404",
                flush=True,
            )
            return []

        response.raise_for_status()

        data = response.json()

    except Exception as exc:
        print(
            f"[proxy-manager] "
            f"source failed "
            f"{source['name']}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return []

    source_type = source["type"]

    if source_type == "proxio":
        results = parse_proxio_json(
            data
        )

    elif source_type == "proxyscrape":
        results = parse_proxyscrape_json(
            data
        )

    else:
        results = []

    results = list(
        dict.fromkeys(
            results
        )
    )

    print(
        f"[proxy-manager] "
        f"source={source['name']} "
        f"candidates={len(results)}",
        flush=True,
    )

    return results


# ============================================================
# Discovery
# ============================================================

def discover_candidates(collection):
    now = utcnow()

    merged = {}

    for source in TEXT_SOURCES:
        for proxy_url in fetch_text_source(
            source
        ):
            merged.setdefault(
                proxy_url,
                set(),
            ).add(
                source["name"]
            )

    for source in JSON_SOURCES:
        for proxy_url in fetch_json_source(
            source
        ):
            merged.setdefault(
                proxy_url,
                set(),
            ).add(
                source["name"]
            )

    if not merged:
        print(
            "[proxy-manager] no proxy candidates discovered",
            flush=True,
        )
        return 0

    operations = []

    for proxy_url, source_names in merged.items():
        operations.append(
            UpdateOne(
                {
                    "url": proxy_url
                },
                {
                    "$set": {
                        "url": proxy_url,
                        "source_country_code": "TW",
                        "last_seen": now,
                    },
                    "$setOnInsert": {
                        "first_seen": now,
                        "is_alive": False,
                        "success_count": 0,
                        "fail_count": 0,
                        "consecutive_failures": 0,
                    },
                    "$addToSet": {
                        "sources": {
                            "$each": sorted(
                                source_names
                            )
                        }
                    },
                },
                upsert=True,
            )
        )

    if operations:
        collection.bulk_write(
            operations,
            ordered=False,
        )

    print(
        f"[proxy-manager] "
        f"merged unique candidates="
        f"{len(merged)}",
        flush=True,
    )

    return len(merged)


# ============================================================
# TCP quick test
# ============================================================

def tcp_check(proxy_url):
    try:
        parsed = urlparse(
            proxy_url
        )

        host = parsed.hostname
        port = parsed.port

        if not host or not port:
            return False, "invalid proxy address"

        start = time.monotonic()

        with socket.create_connection(
            (host, port),
            timeout=TCP_TIMEOUT,
        ):
            pass

        latency_ms = int(
            (
                time.monotonic()
                - start
            )
            * 1000
        )

        return (
            True,
            latency_ms,
        )

    except Exception as exc:
        return (
            False,
            (
                f"{type(exc).__name__}: "
                f"{exc}"
            ),
        )


# ============================================================
# GeoIP
# ============================================================

def lookup_country(exit_ip: str):
    url = (
        f"https://countries.dev/"
        f"ip/{exit_ip}"
    )

    try:
        response = requests.get(
            url,
            timeout=10,
            headers={
                "User-Agent": "recipe-proxy-manager/2.0",
            },
        )

        response.raise_for_status()

        data = response.json()

        country_code = str(
            data.get("countryCode")
            or ""
        ).upper()

        if not country_code:
            print(
                f"[geoip] FAIL "
                f"exit_ip={exit_ip} "
                f"reason=no_country_code",
                file=sys.stderr,
                flush=True,
            )

            return None

        return country_code

    except Exception as exc:
        print(
            f"[geoip] FAIL "
            f"exit_ip={exit_ip} "
            f"error={type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr,
            flush=True,
        )

        return None


# ============================================================
# Actual HTTPS proxy validation
# ============================================================

def validate_one(proxy_url: str):
    # ----------------------------------------
    # Stage 1: TCP
    # ----------------------------------------

    tcp_ok, tcp_result = tcp_check(
        proxy_url
    )

    if not tcp_ok:
        return (
            False,
            None,
            None,
            None,
            f"tcp_failed: {tcp_result}",
        )

    # ----------------------------------------
    # Stage 2: HTTPS request through proxy
    # ----------------------------------------

    session = requests.Session()

    session.trust_env = False

    session.proxies.update(
        {
            "http": proxy_url,
            "https": proxy_url,
        }
    )

    start = time.monotonic()

    try:
        response = session.get(
            (
                "https://api.ipify.org"
                "?format=json"
            ),
            timeout=VALIDATION_TIMEOUT,
            headers={
                "User-Agent": "recipe-proxy-manager/2.0",
            },
        )

        response.raise_for_status()

        data = response.json()

        exit_ip = data.get(
            "ip"
        )

        if not exit_ip:
            return (
                False,
                None,
                None,
                None,
                "ipify returned no ip",
            )

        try:
            ipaddress.ip_address(
                exit_ip
            )
        except ValueError:
            return (
                False,
                exit_ip,
                None,
                None,
                "invalid exit ip",
            )

        latency_ms = int(
            (
                time.monotonic()
                - start
            )
            * 1000
        )

        if (
            latency_ms
            > MAX_LATENCY_MS
        ):
            return (
                False,
                exit_ip,
                None,
                latency_ms,
                (
                    f"latency>"
                    f"{MAX_LATENCY_MS}ms"
                ),
            )

        # ------------------------------------
        # Stage 3: GeoIP
        # ------------------------------------

        country_code = lookup_country(
            exit_ip
        )

        if not country_code:
            return (
                False,
                exit_ip,
                None,
                latency_ms,
                "geoip lookup failed",
            )

        if (
            country_code
            != PROXY_COUNTRY_CODE
        ):
            return (
                False,
                exit_ip,
                country_code,
                latency_ms,
                (
                    f"country="
                    f"{country_code}"
                ),
            )

        return (
            True,
            exit_ip,
            country_code,
            latency_ms,
            None,
        )

    except requests.exceptions.ProxyError as exc:
        error = (
            f"ProxyError: {exc}"
        )

    except requests.exceptions.ConnectTimeout as exc:
        error = (
            f"ConnectTimeout: {exc}"
        )

    except requests.exceptions.ReadTimeout as exc:
        error = (
            f"ReadTimeout: {exc}"
        )

    except requests.exceptions.SSLError as exc:
        error = (
            f"SSLError: {exc}"
        )

    except requests.exceptions.ConnectionError as exc:
        error = (
            f"ConnectionError: {exc}"
        )

    except requests.exceptions.RequestException as exc:
        error = (
            f"RequestException: {exc}"
        )

    except Exception as exc:
        error = (
            f"{type(exc).__name__}: "
            f"{exc}"
        )

    finally:
        session.close()

    return (
        False,
        None,
        None,
        None,
        error,
    )


# ============================================================
# Validation cycle
# ============================================================

def validate_cycle(collection):
    now = utcnow()

    checked_cutoff = (
        now
        - timedelta(
            minutes=20
        )
    )

    stale_cutoff = (
        now
        - timedelta(
            hours=STALE_HOURS
        )
    )

    candidates = list(
        collection.find(
            {
                "$or": [
                    {
                        "last_checked": {
                            "$exists": False
                        }
                    },
                    {
                        "last_checked": {
                            "$lt": checked_cutoff
                        }
                    },
                    {
                        "is_alive": False
                    },
                ],
                "last_seen": {
                    "$gte": stale_cutoff
                },
            },
            {
                "url": 1,
                "fail_count": 1,
                "success_count": 1,
            },
        )
        .sort(
            [
                ("success_count", -1),
                ("fail_count", 1),
            ]
        )
        .limit(
            MAX_VALIDATE_PER_CYCLE
        )
    )

    if not candidates:
        print(
            "[proxy-manager] "
            "no proxies require validation",
            flush=True,
        )

        return (
            0,
            0,
        )

    alive = 0

    with ThreadPoolExecutor(
        max_workers=VALIDATION_WORKERS
    ) as executor:

        future_map = {
            executor.submit(
                validate_one,
                doc["url"],
            ): doc
            for doc in candidates
        }

        for future in as_completed(
            future_map
        ):
            doc = future_map[
                future
            ]

            checked_at = utcnow()

            try:
                (
                    ok,
                    exit_ip,
                    country_code,
                    latency_ms,
                    error,
                ) = future.result()

            except Exception as exc:
                ok = False
                exit_ip = None
                country_code = None
                latency_ms = None

                error = (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

            update = {
                "last_checked": checked_at,
                "exit_ip": exit_ip,
                "country_code": country_code,
                "latency_ms": latency_ms,
                "is_alive": bool(ok),
                "last_error": error,
            }

            if ok:
                alive += 1

                update[
                    "consecutive_failures"
                ] = 0

                update[
                    "last_success"
                ] = checked_at

                collection.update_one(
                    {
                        "_id": doc["_id"]
                    },
                    {
                        "$set": update,
                        "$inc": {
                            "success_count": 1
                        },
                    },
                )

                print(
                    f"[proxy-validator] "
                    f"PASS "
                    f"proxy={doc['url']} "
                    f"exit_ip={exit_ip} "
                    f"country="
                    f"{country_code} "
                    f"latency="
                    f"{latency_ms}ms",
                    flush=True,
                )

            else:
                collection.update_one(
                    {
                        "_id": doc["_id"]
                    },
                    {
                        "$set": update,
                        "$inc": {
                            "fail_count": 1,
                            "consecutive_failures": 1,
                        },
                    },
                )

                print(
                    f"[proxy-validator] "
                    f"FAIL "
                    f"proxy={doc['url']} "
                    f"exit_ip={exit_ip} "
                    f"country="
                    f"{country_code} "
                    f"latency="
                    f"{latency_ms} "
                    f"error={error}",
                    file=sys.stderr,
                    flush=True,
                )

    # ----------------------------------------
    # Disable stale proxies
    # ----------------------------------------

    collection.update_many(
        {
            "last_seen": {
                "$lt": (
                    utcnow()
                    - timedelta(
                        hours=STALE_HOURS
                    )
                )
            }
        },
        {
            "$set": {
                "is_alive": False,
                "last_error": (
                    "stale source entry"
                ),
            }
        },
    )

    return (
        len(candidates),
        alive,
    )


# ============================================================
# Main
# ============================================================

def main():
    client, collection = (
        get_proxy_collection()
    )

    print(
        f"[proxy-manager] started; "
        f"country="
        f"{PROXY_COUNTRY_CODE} "
        f"discovery_interval="
        f"{DISCOVERY_INTERVAL}s "
        f"validation_timeout="
        f"{VALIDATION_TIMEOUT}s "
        f"tcp_timeout="
        f"{TCP_TIMEOUT}s "
        f"max_latency="
        f"{MAX_LATENCY_MS}ms "
        f"workers="
        f"{VALIDATION_WORKERS} "
        f"max_validate="
        f"{MAX_VALIDATE_PER_CYCLE}",
        flush=True,
    )

    try:
        while not _stop:
            discovered = (
                discover_candidates(
                    collection
                )
            )

            checked, alive_now = (
                validate_cycle(
                    collection
                )
            )

            total_alive = (
                collection.count_documents(
                    {
                        "is_alive": True,
                        "country_code": (
                            PROXY_COUNTRY_CODE
                        ),
                    }
                )
            )

            print(
                f"[proxy-manager] "
                f"cycle "
                f"discovered="
                f"{discovered} "
                f"checked="
                f"{checked} "
                f"passed_this_cycle="
                f"{alive_now} "
                f"total_alive_tw="
                f"{total_alive}",
                flush=True,
            )

            for _ in range(
                DISCOVERY_INTERVAL
            ):
                if _stop:
                    break

                time.sleep(1)

    finally:
        client.close()

        print(
            "[proxy-manager] stopped",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )