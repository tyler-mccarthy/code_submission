import java.net.{URL, HttpURLConnection}
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.hadoop.conf.Configuration
import spark.implicits._
import org.apache.hadoop.fs.{FileSystem, Path}

val mockUrlBase = sys.env.getOrElse("MOCKAROO_URL", "https://api.mockaroo.com/api/temperature_data.json")
val apiKey      = sys.env.getOrElse("MOCKAROO_API_KEY", "")
val fetchCnt    = sys.props.getOrElse("fetch.count", "100").toInt
val mockUrl     = s"$mockUrlBase?count=$fetchCnt&key=$apiKey"

// HDFS settings
val hdfsUri       = sys.env.getOrElse("HDFS_URI", "hdfs://localhost:9000")
val historyHdfsPath = sys.env.getOrElse("HDFS_HISTORY_PATH", "hdfs:///data/sensor_history")

val props = new java.util.Properties()
val kafkaServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

// initialise kafka producer
println("Initialising Kafka Producer")
props.put("bootstrap.servers", kafkaServers)
props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
val producer = new KafkaProducer[String, String](props)

// Initialise HDFS client
val hadoopConf = new Configuration()
hadoopConf.set("fs.defaultFS", sys.env.getOrElse("HDFS_URI", "hdfs://localhost:9000"))
val fs = FileSystem.get(hadoopConf)
val historyDir = new Path(historyHdfsPath)
if (!fs.exists(historyDir)) fs.mkdirs(historyDir)

// fetch data from mockaroo
// send to kafka
// appending to HDFS
def fetch_mockaroo_data(): Array[String] = {
   val conn = new URL(mockUrl).openConnection().asInstanceOf[HttpURLConnection]
   conn.setRequestProperty("X-API-Key", apiKey)
   val raw = Source.fromInputStream(conn.getInputStream).mkString
   conn.disconnect()

   val ds = spark.sparkContext.parallelize(Seq(raw))
   val df = spark.read.json(ds)
   df.toJSON.collect()
}

def send_to_kafka(records: Array[String]): Unit = {
    records.foreach { json =>
    producer.send(new ProducerRecord("sensor_topic", null, json))} // check on topic terminology
}

def append_to_HDFS(records: Array[String]): Unit = {
    val ts = System.currentTimeMillis() // mockaroo schema craetion wouldn't enable incremental row_value extraction
    val path = new Path(s"$historyHdfsPath/batch-$ts.json")
    val out = fs.create(path)
    out.writeBytes("[" + records.mkString(",") + "]")
    out.close()
}

def run_pipeline_batch(): Unit = {
    val records = fetch_mockaroo_data()
    if (records.nonEmpty) { 
        append_to_HDFS(records)
        send_to_kafka(records)
        println(s"Pipeline batch: ${records.length} records wrote to HDFS and Kafka")

    } else {
        println("No records fetched from Mockaroo.")
    }
}

run_pipeline_batch()
producer.close()
System.exit(0)