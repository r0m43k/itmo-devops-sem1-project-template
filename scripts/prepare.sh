#!/bin/bash
set -e

sudo apt-get update -y
sudo apt-get install -y postgresql-client curl zip unzip tar

DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-project-sem-1}"
DB_USER="${POSTGRES_USER:-validator}"
DB_PASSWORD="${POSTGRES_PASSWORD:-val1dat0r}"

export PGPASSWORD="$DB_PASSWORD"

for i in $(seq 1 60); do
  if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c 'SELECT 1;' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c 'SELECT 1;' >/dev/null

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<'SQL'
CREATE TABLE IF NOT EXISTS prices (
  id integer,
  name text,
  category text,
  price integer,
  create_date date
);
SQL

echo "OK"
