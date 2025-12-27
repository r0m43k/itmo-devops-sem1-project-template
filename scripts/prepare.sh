#!/bin/bash
set -e

sudo apt update
sudo apt install -y postgresql postgresql-client postgresql-common

if ! pg_lsclusters 2>/dev/null | grep -q '5432'; then
  sudo pg_createcluster 16 main --start
fi

sudo systemctl enable --now postgresql || true
sudo pg_ctlcluster 16 main start || true

for i in {1..20}; do
  if sudo -u postgres psql -c "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

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

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS prices (
  id integer,
  name text,
  category text,
  price integer,
  create_date date
);
SQL

echo "OK"
