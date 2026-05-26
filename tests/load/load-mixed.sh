#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:80}"
TOP_PATH="${TOP_PATH:-/top?n=10}"
INGEST_PATH="${INGEST_PATH:-/ingest}"
RATE_READ="${RATE_READ:-8000}"
RATE_WRITE="${RATE_WRITE:-3000}"
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

echo "🚀 Mixed load test:"
echo "   Read:  GET ${BASE_URL}${TOP_PATH} @ ${RATE_READ}/s"
echo "   Write: POST ${BASE_URL}${INGEST_PATH} @ ${RATE_WRITE}/s"
echo "   Duration: ${DURATION}"

READ_OUT=$(mktemp)
WRITE_OUT=$(mktemp)
trap 'rm -f "$READ_OUT" "$WRITE_OUT"' EXIT

echo "GET ${BASE_URL}${TOP_PATH}" | \
  vegeta attack \
    -rate="${RATE_READ}/s" \
    -duration="${DURATION}" \
    -keepalive=true \
    -timeout="${TIMEOUT}" \
    -connections="${CONNECTIONS}" \
    -output="$READ_OUT" &>/dev/null &

echo "POST ${BASE_URL}${INGEST_PATH}" | \
  vegeta attack \
    -rate="${RATE_WRITE}/s" \
    -duration="${DURATION}" \
    -keepalive=true \
    -timeout="${TIMEOUT}" \
    -connections="${CONNECTIONS}" \
    -body="$PAYLOAD_FILE" \
    -header="Content-Type: application/json" \
    -output="$WRITE_OUT" &>/dev/null &

wait

echo ""
echo "--- Read (GET /top) results ---"
vegeta report -type=text "$READ_OUT"

echo ""
echo "--- Write (POST /ingest) results ---"
vegeta report -type=text "$WRITE_OUT"