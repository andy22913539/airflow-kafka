import os
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

import requests
from pymongo import ASCENDING, ReturnDocument, MongoClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "recipe_ai")
MONGO_APP_USER = os.environ["MONGO_APP_USER"]
MONGO_APP_PASSWORD = os.environ["MONGO_APP_PASSWORD"]
PROXY_COUNTRY_CODE = os.getenv("PROXY_COUNTRY_CODE", "TW").upper()
PROXY_MAX_FAILURES = int(os.getenv("PROXY_MAX_FAILURES", "3"))
PROXY_LEASE_SECONDS = int(os.getenv("PROXY_LEASE_SECONDS", "1800"))


def utcnow():
    return datetime.now(timezone.utc)


def get_proxy_collection():
    uri = (
        f"mongodb://{quote_plus(MONGO_APP_USER)}:{quote_plus(MONGO_APP_PASSWORD)}"
        f"@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_DATABASE}"
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    collection = client[MONGO_DATABASE]["proxy_pool"]
    collection.create_index([("url", ASCENDING)], unique=True)
    collection.create_index([("is_alive", ASCENDING), ("country_code", ASCENDING), ("latency_ms", ASCENDING)])
    collection.create_index([("lease_until", ASCENDING)])
    return client, collection


def lease_proxy(collection, worker_name: str):
    now = utcnow()
    lease_until = now + timedelta(seconds=PROXY_LEASE_SECONDS)
    query = {
        "is_alive": True,
        "country_code": PROXY_COUNTRY_CODE,
        "consecutive_failures": {"$lt": PROXY_MAX_FAILURES},
        "$and": [
            {
                "$or": [
                    {"quarantine_until": {"$exists": False}},
                    {"quarantine_until": None},
                    {"quarantine_until": {"$lte": now}},
                ]
            },
            {
                "$or": [
                    {"lease_until": {"$exists": False}},
                    {"lease_until": None},
                    {"lease_until": {"$lte": now}},
                    {"leased_by": worker_name},
                ]
            },
        ],
    }
    update = {
        "$set": {
            "leased_by": worker_name,
            "lease_until": lease_until,
            "last_used": now,
        }
    }
    return collection.find_one_and_update(
        query,
        update,
        sort=[("latency_ms", ASCENDING), ("consecutive_failures", ASCENDING), ("last_used", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )


def renew_proxy_lease(collection, proxy_id, worker_name: str):
    collection.update_one(
        {"_id": proxy_id, "leased_by": worker_name},
        {
            "$set": {
                "lease_until": utcnow() + timedelta(seconds=PROXY_LEASE_SECONDS),
                "last_used": utcnow(),
            }
        },
    )


def release_proxy(collection, proxy_id, worker_name: str):
    collection.update_one(
        {"_id": proxy_id, "leased_by": worker_name},
        {
            "$set": {"lease_until": utcnow()},
            "$unset": {"leased_by": ""},
        },
    )


def mark_proxy_success(collection, proxy_id, worker_name: str, latency_ms=None):
    now = utcnow()
    set_values = {
        "is_alive": True,
        "consecutive_failures": 0,
        "last_success": now,
        "last_used": now,
        "lease_until": now + timedelta(seconds=PROXY_LEASE_SECONDS),
        "leased_by": worker_name,
    }
    if latency_ms is not None:
        set_values["last_runtime_latency_ms"] = int(latency_ms)
    collection.update_one(
        {"_id": proxy_id},
        {"$set": set_values, "$inc": {"success_count": 1}},
    )


def mark_proxy_failure(collection, proxy_id, worker_name: str, error: str):
    now = utcnow()
    doc = collection.find_one_and_update(
        {"_id": proxy_id},
        {
            "$set": {
                "last_failure": now,
                "last_error": error[:500],
                "lease_until": now,
            },
            "$unset": {"leased_by": ""},
            "$inc": {"fail_count": 1, "consecutive_failures": 1},
        },
        return_document=ReturnDocument.AFTER,
    )
    if doc and int(doc.get("consecutive_failures", 0)) >= PROXY_MAX_FAILURES:
        collection.update_one(
            {"_id": proxy_id},
            {
                "$set": {
                    "is_alive": False,
                    "quarantine_until": now + timedelta(minutes=30),
                }
            },
        )


def build_proxy_session(proxy_url: str) -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.proxies.update({"http": proxy_url, "https": proxy_url})
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.ytower.com.tw/",
        }
    )
    return session
