#!/bin/bash
set -e

sudo apt update
sudo apt install -y postgresql postgresql-client postgresql-common

PG_VER="$(ls /usr/lib/postgresql 2>/dev/null | sort -V | tail -n 1)"
if [ -z "$PG_VER" ]; then
  echo "PostgreSQL binaries not found"
  exit 1
fi

if ! pg_lsclusters 2>/dev/null | awk '{print $1, $2}' | grep -q "^$PG_VER main$"; then
  sudo pg_createcluster "$PG_VER" main
fi

sudo pg_ctlcluster "$PG_VER" main start || true

for i in {1..40}; do
  if PGPASSWORD='' psql -h 127.0.0.1 -U postgres -d postgres -c "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! sudo -u postgres psql -d postgres -c "SELECT 1" >/dev/null 2>&1; then
  echo "PostgreSQL is not responding"
  pg_lsclusters || true
  exit 1
fi

sudo -u postgres psql -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='validator') THEN
    CREATE USER validator WITH PASSWORD 'val1dat0r';
  END IF;
END$$;
SQL

sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='project-sem-1'" | grep -q 1 || \
sudo -u postgres psql -v ON_ERROR_STOP=1 -c 'CREATE DATABASE "project-sem-1" OWNER validator;'

PGPASSWORD=val1dat0r psql -h 127.0.0.1 -U validator -d project-sem-1 -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS prices (
  id integer,
  name text,
  category text,
  price integer,
  create_date date
);
SQL

echo "OK"
