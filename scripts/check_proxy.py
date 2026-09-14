import os
import sys
import requests

proxy = os.getenv("PROXY_URL", "").strip()
expected = os.getenv("PROXY_EXPECTED_IP", "").strip()
name = os.getenv("WORKER_NAME", "worker")

if not proxy:
    print(f"{name}: PROXY_URL is empty", file=sys.stderr)
    raise SystemExit(2)

session = requests.Session()
session.proxies.update({"http": proxy, "https": proxy})
try:
    r = session.get("https://api.ipify.org?format=json", timeout=15)
    r.raise_for_status()
    ip = r.json()["ip"]
except Exception as exc:
    print(f"{name}: proxy FAILED: {exc}", file=sys.stderr)
    raise SystemExit(1)

if expected and ip != expected:
    print(f"{name}: proxy MISMATCH got={ip} expected={expected}", file=sys.stderr)
    raise SystemExit(3)

print(f"{name}: proxy OK exit_ip={ip}")
