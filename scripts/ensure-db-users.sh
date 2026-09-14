#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a
source ./.env
set +a

printf '%s\n' '== Ensure MongoDB application user =='
docker compose exec -T mongodb mongosh \
  --quiet \
  --username "$MONGO_ROOT_USER" \
  --password "$MONGO_ROOT_PASSWORD" \
  --authenticationDatabase admin \
  --eval "
    const d = db.getSiblingDB('$MONGO_DATABASE');
    const u = d.getUser('$MONGO_APP_USER');
    if (u) {
      d.updateUser('$MONGO_APP_USER', {pwd: '$MONGO_APP_PASSWORD', roles:[{role:'readWrite', db:'$MONGO_DATABASE'}]});
      print('Mongo app user updated');
    } else {
      d.createUser({user:'$MONGO_APP_USER', pwd:'$MONGO_APP_PASSWORD', roles:[{role:'readWrite', db:'$MONGO_DATABASE'}]});
      print('Mongo app user created');
    }
  "

printf '%s\n' '== Ensure MySQL application user =='
docker compose exec -T mysql mysql \
  -uroot -p"$MYSQL_ROOT_PASSWORD" \
  --execute="CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\`; CREATE USER IF NOT EXISTS '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD'; ALTER USER '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD'; GRANT ALL PRIVILEGES ON \`$MYSQL_DATABASE\`.* TO '$MYSQL_USER'@'%'; FLUSH PRIVILEGES;"
