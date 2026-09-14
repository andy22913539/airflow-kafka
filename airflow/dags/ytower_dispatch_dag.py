from datetime import datetime, timedelta
import json
import os
from urllib.parse import quote_plus

from airflow import DAG
from airflow.decorators import task
from kafka import KafkaProducer
from pymongo import MongoClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_JOB_TOPIC = os.getenv("KAFKA_JOB_TOPIC", "crawler_jobs")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "recipe_ai")
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_INTERNAL_PORT", "27017"))
MONGO_APP_USER = os.environ.get("MONGO_APP_USER", "")
MONGO_APP_PASSWORD = os.environ.get("MONGO_APP_PASSWORD", "")
MAX_SEQ_NUMBER = int(os.getenv("MAX_SEQ_NUMBER", "5000"))
BACKTRACK_COUNT = int(os.getenv("BACKTRACK_COUNT", "10"))
MAX_NOT_FOUND_LIMIT = int(os.getenv("MAX_NOT_FOUND_LIMIT", "50"))
DAG_SCHEDULE = os.getenv("YTOWER_DAG_SCHEDULE", "").strip() or None


def generate_prefixes(letters="ABCDEFGHI", num1_range=(1, 10)):
    return [
        f"{letter}{n1:02d}"
        for letter in letters
        for n1 in range(num1_range[0], num1_range[1] + 1)
    ]


def get_mongo_collection():
    uri = (
        f"mongodb://{quote_plus(MONGO_APP_USER)}:{quote_plus(MONGO_APP_PASSWORD)}"
        f"@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_DATABASE}"
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    return client, client[MONGO_DATABASE]["recipes"]


def latest_numeric_seq(collection, prefix):
    pipeline = [
        {"$match": {"SEQ": {"$regex": f"^{prefix}-\\d+$"}}},
        {
            "$project": {
                "n": {
                    "$convert": {
                        "input": {"$arrayElemAt": [{"$split": ["$SEQ", "-"]}, 1]},
                        "to": "int",
                        "onError": 0,
                        "onNull": 0,
                    }
                }
            }
        },
        {"$group": {"_id": None, "max_n": {"$max": "$n"}}},
    ]
    row = next(collection.aggregate(pipeline), None)
    return int(row["max_n"]) if row and row.get("max_n") else 0


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ytower_recipe_dispatch_proxy_pool",
    default_args=default_args,
    description="Airflow 派發 YTower 工作到 Kafka；4 workers 從動態台灣 Proxy Pool 取代理，再由 Kafka 寫入 MongoDB",
    schedule_interval=DAG_SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crawler", "ytower", "kafka", "proxy-pool", "taiwan"],
) as dag:

    @task()
    def dispatch_jobs():
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=5,
        )
        mongo_client, collection = get_mongo_collection()
        count = 0
        try:
            for prefix in generate_prefixes():
                latest = latest_numeric_seq(collection, prefix)
                start_num = max(1, latest - BACKTRACK_COUNT) if latest else 1
                job = {
                    "prefix": prefix,
                    "start_num": start_num,
                    "end_num": MAX_SEQ_NUMBER,
                    "max_not_found_limit": MAX_NOT_FOUND_LIMIT,
                }
                producer.send(KAFKA_JOB_TOPIC, value=job)
                count += 1
                print(f"dispatch {prefix}: start={start_num}")
            producer.flush()
            print(f"dispatched {count} jobs to {KAFKA_JOB_TOPIC}")
            return count
        finally:
            producer.close()
            mongo_client.close()

    dispatch_jobs()
