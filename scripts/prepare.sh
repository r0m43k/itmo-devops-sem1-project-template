#!/bin/bash
set -e

sudo apt update
sudo apt install -y postgresql postgresql-client curl zip unzip tar

sudo systemctl enable --now postgresql

sudo -u postgres psql <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='validator') THEN
    CREATE USER validator WITH PASSWORD 'val1dat0r';
  END IF;
END$$;
SQL

sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='project-sem-1'" | grep -q 1 || \
sudo -u postgres psql -c 'CREATE DATABASE "project-sem-1" OWNER validator;'

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 <<'SQL'
CREATE TABLE IF NOT EXISTS prices (
  id integer,
  name text,
  category text,
  price integer,
  create_date date
);
SQL

echo "OK"
