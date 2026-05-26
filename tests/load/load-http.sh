#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:80}"
TOP_PATH="${TOP_PATH:-/top?n=10}"
RATE="${RATE:-10000}"
DURATION="${DURATION:-30s}"
TIMEOUT="${TIMEOUT:-5s}"
CONNECTIONS="${CONNECTIONS:-4096}"

if ! command -v vegeta &> /dev/null; then
    echo " vegeta not found. Install: https://github.com/tsenart/vegeta#install"
    exit 1
fi

echo "🚀 HTTP read test: GET ${BASE_URL}${TOP_PATH}"
echo "   Rate: ${RATE}/s, Duration: ${DURATION}, Connections: ${CONNECTIONS}"

echo "GET ${BASE_URL}${TOP_PATH}" | \
  vegeta attack \
    -rate="${RATE}/s" \
    -duration="${DURATION}" \
    -keepalive=true \
    -timeout="${TIMEOUT}" \
    -connections="${CONNECTIONS}" \
  | vegeta report -type=text