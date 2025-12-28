#!/bin/bash
set -e

APP_PORT=8080
APP_URL="http://localhost:${APP_PORT}/api/v0/prices"

go build -o project_sem .

./project_sem > server.log 2>&1 &
echo $! > server.pid

for i in $(seq 1 20); do
  if curl -s -o /dev/null -w "%{http_code}" "$APP_URL" | grep -qE "200|405"; then
    echo "Server is up"
    exit 0
  fi
  sleep 0.5
done

echo "Server did not start"
echo "---- server.log ----"
cat server.log || true
exit 1
