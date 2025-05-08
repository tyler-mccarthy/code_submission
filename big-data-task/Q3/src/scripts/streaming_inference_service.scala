import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.ml.PipelineModel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

val hdfsUri = sys.props.getOrElse("hdfs.uri", "hdfs://localhost:9000")
val kafkaServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
val topic = sys.env.getOrElse("KAFKA_TOPIC", "sensor_topic")
val modelPath = sys.env.getOrElse("MODEL_PATH", s"$hdfsUri/models/sensorStatusModel")
val outputPath = sys.env.getOrElse("PREDICTIONS_PATH", s"$hdfsUri/data/sensor_predictions")
val metricsPath = sys.env.getOrElse("METRICS_PATH", s"$hdfsUri/data/sensor_predictions/metrics")
val triggerIntervalMs = sys.props.getOrElse("trigger.ms", "10000").toLong
val checkpointDir  = s"$outputPath/checkpoints"

val spark = SparkSession.builder()
  .appName("StreamingInference")
  .master("local[*]")
  .config("spark.sql.shuffle.partitions", "8")
  .config("fs.defaultFS", hdfsUri)
  .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
import spark.implicits._

val hconf = new Configuration(); hconf.set("fs.defaultFS", hdfsUri)
val fs = FileSystem.get(hconf)
for (dir <- Seq(outputPath, checkpointDir, metricsPath)) {
  val p = new Path(dir)
  if (!fs.exists(p)) fs.mkdirs(p)
}

val schema = new StructType()
  .add("sensor_id", IntegerType)
  .add("temperature", DoubleType)
  .add("humidity", DoubleType)
  .add("pressure", DoubleType)
  .add("status", StringType)
  .add("ts", TimestampType)

// read from kafka
val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaServers)
  .option("subscribe", topic)
  .option("startingOffsets", "earliest") // would remove in production
  .option("maxOffsetsPerTrigger", "1000")
  .load()

val events = kafkaDF
  .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts")
  .select(from_json(col("json"), schema).as("data"), col("kafka_ts"))
  .select("data.*", "kafka_ts")
  .withColumn("ingest_time", current_timestamp())

// Load Model
val model = PipelineModel.load(modelPath)

val predictions = model.transform(events)
    .withColumn("process_time", current_timestamp())
    .withColumn("latency_ms", (col("process_time").cast("long") - col("kafka_ts").cast("long")) * 1000)
    .select("sensor_id","temperature","humidity","pressure","ts","kafka_ts","process_time","prediction","probability","latency_ms")

var totalCount = 0L
val startMS = System.currentTimeMillis()

val query = predictions.writeStream
    .foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>
        println(s"[StreamingInference] starting foreachBatch for batchId=$batchId")
        val count = batchDF.count()
        if (count > 0) {
            totalCount += count
            val avgLatency = batchDF.agg(avg("latency_ms")).as[Double].first()
            val alertPct = batchDF.filter($"prediction" === 1).count() * 100.0 / count
            val elapsedSec = (System.currentTimeMillis() - startMS) / 1000.0
            val throughput = totalCount / elapsedSec
            println(s"[StreamingInference] batch $batchId: count=$count, avgLat=$avgLatency, alertPct=$alertPct, thru=$throughput")
      // Save metrics file
      // val metricsFile = new Path(s"$metricsPath/metrics-$batchId.txt")
      // if (fs.exists(metricsFile)) fs.delete(metricsFile, false)
      // val out = fs.create(metricsFile)
      import spark.implicits._
      val metricsDF = Seq((batchId, count, avgLatency, alertPct, throughput))
        .toDF("batchId", "count", "avgLatency", "throughput")
      metricsDF.coalesce(1).write.mode("append").parquet(s"$metricsPath/parquet")
      // Append data
      batchDF.write
        .mode("append")
        .partitionBy("prediction")
        .parquet(outputPath)
    }
  }
  .option("checkpointLocation", checkpointDir)
  // prod usage
  // .trigger(Trigger.ProcessingTime(s"${triggerIntervalMs} milliseconds"))
  // consume all of kafka
  .trigger(Trigger.Once())
  .start()

println("Streaming inference started...")
query.awaitTermination()

spark.stop()

System.exit(0)