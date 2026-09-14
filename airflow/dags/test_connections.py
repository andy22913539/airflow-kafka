import os
from urllib.parse import quote_plus

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from kafka import KafkaAdminClient
from pymongo import MongoClient
import pymysql


default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="test_connections_pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["diagnostic"],
) as dag:

    @task()
    def test_all():
        kafka = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            client_id="airflow-connection-test",
        )
        topics = set(kafka.list_topics())
        kafka.close()
        required = {
            os.getenv("KAFKA_JOB_TOPIC", "crawler_jobs"),
            os.getenv("KAFKA_RESULT_TOPIC", "ytower_recipe_results"),
        }
        missing = required - topics
        if missing:
            raise RuntimeError(f"Kafka topics missing: {sorted(missing)}")

        mongo_db = os.getenv("MONGO_DATABASE", "recipe_ai")
        mongo_user = os.environ["MONGO_APP_USER"]
        mongo_password = os.environ["MONGO_APP_PASSWORD"]
        mongo_uri = (
            f"mongodb://{quote_plus(mongo_user)}:{quote_plus(mongo_password)}"
            f"@mongodb:27017/{mongo_db}?authSource={mongo_db}"
        )
        mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        mongo.admin.command("ping")
        mongo.close()

        mysql = pymysql.connect(
            host="mysql",
            port=3306,
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            database=os.environ["MYSQL_DATABASE"],
            connect_timeout=10,
        )
        with mysql.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
        mysql.close()

        print("OK: Kafka + MongoDB + MySQL")
        return True

    test_all()
