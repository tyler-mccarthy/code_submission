import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

val spark = SparkSession.builder()
    .appName("CollectInferenceMetrics")
    .master("local[*]")
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._

val metrics_directory = sys.props.getOrElse("hdfs.metrics", "/data/sensor_predictions/metrics") + "/parquet"


val all_metrics = spark.read.parquet(metrics_directory)
all_metrics.orderBy($"batchId").show(truncate = false)

println("Per-batch inference metrics: ")
parsed_text.orderBy($"batchId").show(false)

val summary_stats = all_metrics.agg(
    count("*").as("numBatches"),
    sum("count").as("totalRecords"),
    round(avg("avgLatency"),2).as("meanLatency_ms"),
    round(max("avgLatency"),2).as("maxLatency_ms"),
    round(min("avgLatency"),2).as("minLatency_ms"),
    round(avg("alertPct"),2).as("meanAlertPct"),
    round(avg("throughput"),2).as("meanThroughput_per_s")
)

println("Summary of inference-metrics:")
summary_stats.show(truncate = false)

spark.stop()