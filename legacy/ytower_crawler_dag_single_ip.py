from datetime import datetime, timedelta
import json
import os
import random
import sqlite3
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.decorators import task

# ============================================================
# 基本設定 (建議將路徑設定為 Airflow 專用的數據資料夾)
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "ytower_recipes.db")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "progress_checkpoint.json")
FIRST_RUN_FLAG = os.path.join(BASE_DIR, "first_run_completed.flag")
EXPORT_STATE_FILE = os.path.join(BASE_DIR, "export_sync_state.json")

OUTPUT_CSV = os.path.join(BASE_DIR, "ytower_seq_recipes.csv")
OUTPUT_JSON = os.path.join(BASE_DIR, "ytower_seq_recipes.json")

MAX_NOT_FOUND_LIMIT = 50
BACKTRACK_COUNT = 10
SAVE_BATCH_SIZE = 50
COOLDOWN_SUCCESS_COUNT = 50
REQUEST_TIMEOUT = 20
MAX_SEQ_NUMBER = 5000


# ============================================================
# 核心邏輯函式 (SQLite, Checkpoint, Sync, Crawler)
# ============================================================

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            SEQ TEXT NOT NULL UNIQUE,
            食譜名稱 TEXT,
            上線日期 TEXT,
            關鍵字 TEXT,
            食譜網址 TEXT,
            材料 TEXT,
            做法步驟 TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn

def recipe_exists(conn, seq):
    cursor = conn.execute("SELECT 1 FROM recipes WHERE SEQ = ? LIMIT 1", (seq,))
    return cursor.fetchone() is not None

def insert_recipe(conn, data):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO recipes (
            SEQ, 食譜名稱, 上線日期, 關鍵字, 食譜網址, 材料, 做法步驟, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["SEQ"], data["食譜名稱"], data["上線日期"],
            data["關鍵字"], data["食譜網址"], data["材料"],
            data["做法步驟"], now
        ),
    )
    return cursor.rowcount == 1

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): int(v) for k, v in data.items()}
    except Exception as e:
        print(f"⚠️ checkpoint 讀取失敗：{e}")
    return {}

def save_checkpoint(checkpoint):
    temp_file = CHECKPOINT_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=4)
    os.replace(temp_file, CHECKPOINT_FILE)

def generate_prefixes(letters="ABCDEFGHI", num1_range=(1, 10)):
    prefixes = []
    for letter in letters:
        for n1 in range(num1_range[0], num1_range[1] + 1):
            prefixes.append(f"{letter}{n1:02d}")
    return prefixes

def generate_seq_for_prefix(prefix, start_num, end_num=MAX_SEQ_NUMBER):
    for n2 in range(start_num, end_num + 1):
        yield f"{prefix}-{n2:04d}"
        if n2 < 1000:
            yield f"{prefix}-{n2:03d}"

def parse_recipe_by_seq(seq, session):
    url = f"https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq={seq}"
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.encoding = "big5"
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        title_el = soup.select_one("#recipe_name h2 a")
        if not title_el or not title_el.text.strip():
            return None

        title = title_el.text.strip()
        time_el = soup.select_one("#recipe_info time")
        publish_date = (time_el.get("datetime", "").strip() or time_el.text.strip()) if time_el else ""

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
            "食譜名稱": title,
            "上線日期": publish_date,
            "關鍵字": ", ".join(keywords),
            "食譜網址": url,
            "材料": " | ".join(ingredients),
            "做法步驟": "\n".join(steps),
        }
    except Exception as e:
        print(f" └─ 錯誤 ({seq}): {e}")
        return None

def get_recipes_after_id(conn, last_id):
    cursor = conn.execute(
        """
        SELECT id, SEQ, 食譜名稱, 上線日期, 關鍵字, 食譜網址, 材料, 做法步驟
        FROM recipes WHERE id > ? ORDER BY id
        """,
        (last_id,),
    )
    columns = ["id", "SEQ", "食譜名稱", "上線日期", "關鍵字", "食譜網址", "材料", "做法步驟"]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def load_export_state():
    if not os.path.exists(EXPORT_STATE_FILE):
        return {"last_exported_id": 0}
    try:
        with open(EXPORT_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"last_exported_id": int(data.get("last_exported_id", 0))}
    except Exception:
        return {"last_exported_id": 0}

def save_export_state(last_exported_id):
    temp_file = EXPORT_STATE_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump({"last_exported_id": last_exported_id}, f, indent=4)
    os.replace(temp_file, EXPORT_STATE_FILE)

def append_to_csv(data):
    if not data:
        return
    columns = ["SEQ", "食譜名稱", "上線日期", "關鍵字", "食譜網址", "材料", "做法步驟"]
    df = pd.DataFrame(data, columns=columns)
    file_exists = os.path.exists(OUTPUT_CSV)
    df.to_csv(
        OUTPUT_CSV,
        mode="a" if file_exists else "w",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig",
    )

def append_to_json_array(data):
    if not data:
        return
    clean_data = [{
        "SEQ": item["SEQ"], "食譜名稱": item["食譜名稱"], "上線日期": item["上線日期"],
        "關鍵字": item["關鍵字"], "食譜網址": item["食譜網址"], "材料": item["材料"], "做法步驟": item["做法步驟"]
    } for item in data]

    existing_data = []
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            if not isinstance(existing_data, list):
                existing_data = []
        except Exception:
            existing_data = []

    existing_data.extend(clean_data)
    temp_file = OUTPUT_JSON + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)
    os.replace(temp_file, OUTPUT_JSON)

def sync_export_files(conn):
    state = load_export_state()
    last_exported_id = state["last_exported_id"]
    new_data = get_recipes_after_id(conn, last_exported_id)
    if not new_data:
        return 0

    append_to_csv(new_data)
    append_to_json_array(new_data)
    latest_id = new_data[-1]["id"]
    save_export_state(latest_id)
    return len(new_data)

def crawl_prefix(prefix, start_num, session, conn, checkpoint):
    consecutive_not_found = 0
    last_found_num = start_num - 1
    new_count = 0
    success_count = 0

    for seq in generate_seq_for_prefix(prefix, start_num, MAX_SEQ_NUMBER):
        data = parse_recipe_by_seq(seq, session)

        if data is None:
            consecutive_not_found += 1
            if consecutive_not_found >= MAX_NOT_FOUND_LIMIT:
                checkpoint[prefix] = max(checkpoint.get(prefix, 0), last_found_num)
                save_checkpoint(checkpoint)
                conn.commit()
                sync_export_files(conn)
                return new_count
            time.sleep(random.uniform(0.5, 1.2))
            continue

        consecutive_not_found = 0
        try:
            seq_num = int(seq.split("-")[1])
        except Exception:
            seq_num = start_num

        last_found_num = max(last_found_num, seq_num)
        checkpoint[prefix] = max(checkpoint.get(prefix, 0), last_found_num)

        if recipe_exists(conn, seq):
            pass
        else:
            if insert_recipe(conn, data):
                new_count += 1

        success_count += 1
        save_checkpoint(checkpoint)

        if success_count % 10 == 0:
            conn.commit()

        if new_count > 0 and new_count % SAVE_BATCH_SIZE == 0:
            conn.commit()
            sync_export_files(conn)

        time.sleep(random.uniform(2.5, 5.0))

        if success_count > 0 and success_count % COOLDOWN_SUCCESS_COUNT == 0:
            time.sleep(random.uniform(10.0, 20.0))

    conn.commit()
    save_checkpoint(checkpoint)
    sync_export_files(conn)
    return new_count


# ============================================================
# Airflow DAG 定義
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ytower_recipe_crawler",
    default_args=default_args,
    description="每 7 天定期抓取 YTOWER 最新食譜並同步 CSV/JSON",
    schedule_interval="0 0 */7 * *",  # 每 7 天午夜 12 點執行一次
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crawler", "ytower"],
) as dag:

    @task()
    def check_and_repair():
        """任務 1: 檢查並補足未同步至 CSV/JSON 的 SQLite 資料"""
        conn = get_db_connection()
        try:
            synced_count = sync_export_files(conn)
            print(f"🔧 已自動補同步 {synced_count} 筆。")
        finally:
            conn.close()

    @task()
    def run_crawler():
        """任務 2: 執行增量與 Prefix 爬蟲主程式"""
        checkpoint = load_checkpoint()
        first_run_completed = os.path.exists(FIRST_RUN_FLAG)

        conn = get_db_connection()
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.ytower.com.tw/",
        })

        prefixes = generate_prefixes(letters="ABCDEFGHI", num1_range=(1, 10))
        total_new = 0

        try:
            for prefix in prefixes:
                if prefix in checkpoint:
                    last_found = checkpoint[prefix]
                    start_num = max(1, last_found - BACKTRACK_COUNT)
                else:
                    start_num = 1

                new_count = crawl_prefix(
                    prefix=prefix,
                    start_num=start_num,
                    session=session,
                    conn=conn,
                    checkpoint=checkpoint,
                )
                total_new += new_count
                conn.commit()

            save_checkpoint(checkpoint)
            sync_export_files(conn)

            if not first_run_completed:
                with open(FIRST_RUN_FLAG, "w", encoding="utf-8") as f:
                    f.write(f"First run completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            print(f"🎉 爬蟲執行完成！本次共新增 {total_new} 筆資料。")

        finally:
            conn.close()
            session.close()

    # 設定 Task 依賴順序
    check_and_repair() >> run_crawler()