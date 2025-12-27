#!/bin/bash
set -e
nohup go run . >/tmp/app.log 2>&1 &
sleep 1
