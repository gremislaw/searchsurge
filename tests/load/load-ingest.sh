#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:80}"
INGEST_PATH="${INGEST_PATH:-/ingest}"
RATE="${RATE:-5000}"
DURATION="${DURATION:-30s}"
TIMEOUT="${TIMEOUT:-5s}"
CONNECTIONS="${CONNECTIONS:-200}"
PAYLOAD_FILE="${PAYLOAD_FILE:-./tests/load/ingest_payload.json}"

if ! command -v vegeta &> /dev/null; then
    echo " vegeta not found. Install: https://github.com/tsenart/vegeta#install"
    exit 1
fi

if [ ! -f "$PAYLOAD_FILE" ]; then
    echo " Payload file not found: $PAYLOAD_FILE"
    exit 1
fi

echo "🚀 Ingest test: POST ${BASE_URL}${INGEST_PATH}"
echo "   Rate: ${RATE}/s, Duration: ${DURATION}, Connections: ${CONNECTIONS}"

echo "POST ${BASE_URL}${INGEST_PATH}" | \
  vegeta attack \
    -rate="${RATE}/s" \
    -duration="${DURATION}" \
    -keepalive=true \
    -timeout="${TIMEOUT}" \
    -connections="${CONNECTIONS}" \
    -body="$PAYLOAD_FILE" \
    -header="Content-Type: application/json" \
  | vegeta report -type=text