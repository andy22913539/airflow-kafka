from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.decorators import task
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_JOB_TOPIC = os.getenv("KAFKA_JOB_TOPIC", "crawler_jobs")
MAX_SEQ_NUMBER = int(os.getenv("MAX_SEQ_NUMBER", "5000"))
MAX_NOT_FOUND_LIMIT = int(os.getenv("MAX_NOT_FOUND_LIMIT", "50"))
DAG_SCHEDULE = os.getenv("YTOWER_DAG_SCHEDULE", "").strip() or None


def generate_prefixes(letters="ABCDEFGHI", num1_range=(1, 10)):
    return [
        f"{letter}{n1:02d}"
        for letter in letters
        for n1 in range(num1_range[0], num1_range[1] + 1)
    ]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ytower_recipe_dispatch_full_run",
    default_args=default_args,
    description="Full YTower rerun: dispatch all 90 prefixes from numeric SEQ 1 to Kafka",
    schedule_interval=DAG_SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crawler", "ytower", "kafka", "full-run", "direct", "proxy"],
) as dag:

    @task()
    def dispatch_jobs():
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=5,
        )
        count = 0
        try:
            for prefix in generate_prefixes():
                # 明確忽略 MongoDB 與任何 checkpoint；每次 DAG 都從 1 完整重跑。
                job = {
                    "prefix": prefix,
                    "start_num": 1,
                    "end_num": MAX_SEQ_NUMBER,
                    "max_not_found_limit": MAX_NOT_FOUND_LIMIT,
                }
                producer.send(KAFKA_JOB_TOPIC, value=job)
                count += 1
                print(f"dispatch full-run {prefix}: start=1 end={MAX_SEQ_NUMBER}")
            producer.flush()
            print(f"dispatched {count} full-run jobs to {KAFKA_JOB_TOPIC}")
            return count
        finally:
            producer.close()

    dispatch_jobs()
