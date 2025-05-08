#!/usr/bin/env bash
set -eou pipefail


if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <FetchCount> <StreamSeconds>"
  exit 1
fi

# env var configuration
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
start-dfs.sh
FETCH_CNT=${1:-500}
STREAM_SECS=$20
export MOCKAROO_API_KEY="${MOCKAROO_API_KEY:-a3ff6890}"
export MOCKAROO_URL="${MOCKAROO_URL:-https://my.api.mockaroo.com/temperature_data.json}"

echo "==> 1) Produce data from Mockaroo (count=$FETCH_CNT)"
spark-shell --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  --conf "spark.driver.extraJavaOptions=-Dhdfs.uri=hdfs://localhost:9000 -Dfetch.count=$FETCH_CNT" \
  -i src/scripts/sensor_data.scala
hdfs dfs -mkdir -p /data/sensor_history 

echo
echo "==> 2) Train model"
spark-shell --master local[*] \
  --conf "spark.driver.extraJavaOptions=-Dhdfs.uri=hdfs://localhost:9000 -Dhdfs.history=/data/sensor_history/*.json -Dhdfs.model=/models/sensorStatusModel" \
  -i src/scripts/model_trainer.scala

echo
echo "==> 3) Start streaming inference (runs for $STREAM_SECS seconds)"
spark-shell --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  --conf "spark.driver.extraJavaOptions=-Dhdfs.uri=hdfs://localhost:9000 -Dhdfs.predictions=/data/sensor_predictions -Dhdfs.metrics=/data/sensor_predictions/metrics -Dtrigger.ms=10000" \
  -i src/scripts/streaming_inference_service.scala &

STREAM_PID=$!
sleep "$STREAM_SECS"
echo "==> Stopping streaming (PID $STREAM_PID)"
kill "$STREAM_PID" || true

echo
echo "==> 4) Collect metrics & generate report"
spark-shell --master local[*] \
  --conf "spark.driver.extraJavaOptions=-Dhdfs.uri=hdfs://localhost:9000 -Dhdfs.metrics=/data/sensor_predictions/metrics" \
  -i src/scripts/collect_inference_metrics.scala

echo
echo "=== Pipeline complete ==="
