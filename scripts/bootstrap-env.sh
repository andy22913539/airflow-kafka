#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  echo ".env already exists; not overwriting"
  exit 0
fi

cp .env.example .env
python3 - <<'PY'
from pathlib import Path
import secrets
try:
    from cryptography.fernet import Fernet
    fernet = Fernet.generate_key().decode()
except Exception:
    import base64, os
    fernet = base64.urlsafe_b64encode(os.urandom(32)).decode()

p = Path('.env')
s = p.read_text()
replacements = {
    'CHANGE_ME_AIRFLOW_ADMIN': secrets.token_urlsafe(24),
    'CHANGE_ME_GENERATE_WITH_FERNET': fernet,
    'CHANGE_ME_RANDOM_SECRET': secrets.token_urlsafe(48),
    'CHANGE_ME_POSTGRES': secrets.token_urlsafe(24),
    'CHANGE_ME_MYSQL_ROOT': secrets.token_urlsafe(24),
    'CHANGE_ME_MYSQL_APP': secrets.token_urlsafe(24),
    'CHANGE_ME_MONGO_ROOT': secrets.token_urlsafe(24),
    'CHANGE_ME_MONGO_APP': secrets.token_urlsafe(24),
}
for a, b in replacements.items():
    s = s.replace(a, b)
p.write_text(s)
PY
chmod 600 .env
echo "Created .env with generated secrets. Dynamic TW proxy discovery is enabled by default."
