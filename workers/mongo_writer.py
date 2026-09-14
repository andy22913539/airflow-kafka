"""Kafka result topic -> MongoDB writer.

這支程式沿用前一版 consume_kafka_to_mongodb 的核心設計，
但改成常駐 service，避免 Airflow task 10 秒 timeout 時 crawler 還沒完成。
"""

import json
import os
import signal
import sys
from datetime import datetime
from urllib.parse import quote_plus

from kafka import KafkaConsumer
from pymongo import ASCENDING, MongoClient, UpdateOne

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "ytower_recipe_results")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ytower-mongo-writer")
MONGO_BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "50"))

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "recipe_ai")
MONGO_APP_USER = os.environ["MONGO_APP_USER"]
MONGO_APP_PASSWORD = os.environ["MONGO_APP_PASSWORD"]

_stop = False


def _handle_signal(signum, frame):
    global _stop
    _stop = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def get_mongo_collection():
    uri = (
        f"mongodb://{quote_plus(MONGO_APP_USER)}:{quote_plus(MONGO_APP_PASSWORD)}"
        f"@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_DATABASE}"
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    collection = client[MONGO_DATABASE]["recipes"]
    collection.create_index([("SEQ", ASCENDING)], unique=True)
    collection.create_index([("prefix", ASCENDING), ("seq_num", ASCENDING)])
    return client, collection


def flush_batch(collection, consumer, batch):
    if not batch:
        return 0

    collection.bulk_write(batch, ordered=False)
    # MongoDB 成功寫入後才提交 Kafka offset，避免訊息寫入失敗卻被標成已消費。
    consumer.commit()
    count = len(batch)
    batch.clear()
    print(f"[mongo-writer] upserted {count} records and committed offsets", flush=True)
    return count


def main() -> int:
    client, collection = get_mongo_collection()

    consumer = KafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        max_poll_records=MONGO_BATCH_SIZE,
    )

    print(
        f"[mongo-writer] consuming Kafka topic={KAFKA_RESULT_TOPIC} -> "
        f"MongoDB={MONGO_DATABASE}.recipes",
        flush=True,
    )

    batch = []
    total = 0
    try:
        while not _stop:
            records = consumer.poll(timeout_ms=1000, max_records=MONGO_BATCH_SIZE)

            for _, messages in records.items():
                for message in messages:
                    doc = message.value
                    if not isinstance(doc, dict) or not doc.get("SEQ"):
                        print("[mongo-writer] skip invalid Kafka message without SEQ", flush=True)
                        continue

                    doc["updated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                    batch.append(
                        UpdateOne(
                            {"SEQ": doc["SEQ"]},
                            {"$set": doc},
                            upsert=True,
                        )
                    )

            if batch:
                total += flush_batch(collection, consumer, batch)

        if batch:
            total += flush_batch(collection, consumer, batch)

        print(f"[mongo-writer] stopped; total upserted={total}", flush=True)
        return 0

    except Exception as exc:
        # 不 commit；重啟後 Kafka 會重新投遞尚未確認的訊息。
        print(f"[mongo-writer] fatal error: {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
