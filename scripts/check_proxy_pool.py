import os
from urllib.parse import quote_plus

from pymongo import MongoClient

host = os.getenv("MONGO_HOST", "mongodb")
port = int(os.getenv("MONGO_PORT", "27017"))
db_name = os.getenv("MONGO_DATABASE", "recipe_ai")
user = os.environ["MONGO_APP_USER"]
password = os.environ["MONGO_APP_PASSWORD"]
country = os.getenv("PROXY_COUNTRY_CODE", "TW").upper()

uri = f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db_name}?authSource={db_name}"
client = MongoClient(uri, serverSelectionTimeoutMS=10000)
collection = client[db_name]["proxy_pool"]

alive = collection.count_documents({"is_alive": True, "country_code": country})
print(f"verified alive {country} proxies: {alive}")
for doc in collection.find(
    {"is_alive": True, "country_code": country},
    {"url": 1, "exit_ip": 1, "latency_ms": 1, "sources": 1, "_id": 0},
).sort("latency_ms", 1).limit(20):
    print(doc)

client.close()
raise SystemExit(0 if alive > 0 else 2)
