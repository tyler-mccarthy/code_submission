#!/usr/bin/env bash
set -euo pipefail

MOCKAROO_API_KEY="${MOCKAROO_API_KEY:-e4365cb0}"
MOCKAROO_URL="${MOCKAROO_URL:-https://my.api.mockaroo.com/temperature_data.json}"
FETCH_CNT=${1:-100}
TOPIC=${2:-sensor_topic}

curl -s "${MOCKAROO_URL}?count=${FETCH_CNT}&key=${MOCKAROO_API_KEY}" \
  | "${KAFKA_HOME:-/usr/local/kafka}/bin/kafka-console-producer.sh" \
      --broker-list "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}" \
      --topic "${TOPIC}"