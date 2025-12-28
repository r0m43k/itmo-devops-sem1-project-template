#!/bin/bash
set -e

APP_PORT="${APP_PORT:-8080}"

go build -o project_sem .

pkill project_sem 2>/dev/null || true
./project_sem > app.log 2>&1 &

for i in $(seq 1 30); do
  if curl -s "http://localhost:${APP_PORT}/api/v0/prices" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "App did not start. Logs:"
tail -n 200 app.log || true
exit 1
