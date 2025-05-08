import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.PipelineModel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.io.PrintWriter

// first iteration of model pipeline (unused)
// // Model pipeline
// val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.01).setElasticNetParam(0.5).setLabelCol("label").setFeaturesCol("features")
// val pipeline = new Pipeline().setStages(Array(indexer,assembler,lr))

// // Train
// println("Training model...")
// val start = System.currentTimeMillis()
// val model = pipeline.fit(trainingData)
// val duration = (System.currentTimeMillis()-start)/1000.0
// println(f"Training completed in $duration%.2f s")


val spark = SparkSession.builder()
    .appName("SensorModelTraining")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
import spark.implicits._

val hdfsUri = sys.props("hdfs.uri")
val historyPath = sys.props("hdfs.history")
val modelPath = sys.props("hdfs.model")
val modelDir = modelPath.stripSuffix("/sensorStatusModel")
val conf = new Configuration()
conf.set("fs.defaultFS", hdfsUri)
val fs = FileSystem.get(conf)
val dirPath = new Path(modelDir)
if (!fs.exists(dirPath)) {
    fs.mkdirs(dirPath)
    println(s"Created directory: $modelDir")
}

println(s"Reading data from $historyPath")
val raw = spark.read.option("multiLine", true).json(historyPath)
raw.printSchema()
raw.show(5)
println(s"Total records: ${raw.count()}")

val numericCols = Seq("temperature", "humidity", "pressure")
raw.select(
    count("*").as("total"),
    count("sensor_id").as("nonNullSensorIds"),
    count("status").as("nonNullStatus"),
    count(when(col("status")==="NORMAL",1)).as("normalCount"),
    count(when(col("status")==="ALERT",1)).as("alertCount")
).show()
raw.describe(numericCols:_*).show()

// Preprocessing pipeline
val indexer = new StringIndexer().setInputCol("status").setOutputCol("label").setHandleInvalid("skip") //remove/clean any other labels
val assembler = new VectorAssembler().setInputCols(Array("temperature","humidity","pressure")).setOutputCol("features").setHandleInvalid("keep")

//setup random forest classifier
val rf = new RandomForestClassifier() 
    .setLabelCol("label")
    .setFeaturesCol("features")

val paramGrid = new ParamGridBuilder()
    .addGrid(rf.numTrees, Array(10, 20))
    .addGrid(rf.maxDepth, Array(5, 10))
    .build()

val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("rawPrediction")
    .setMetricName("areaUnderROC")

// cv with 3 fold and two model parrallelism
val cv = new CrossValidator()
    .setEstimator(new Pipeline().setStages(Array(indexer, assembler, rf)))
    .setEvaluator(evaluator) 
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(3)
    .setParallelism(2)

// Split
val Array(trainingData,testData) = raw.randomSplit(Array(0.8,0.2), seed=42)
println(s"Training: ${trainingData.count()}, Test: ${testData.count()}")

// Model Pipeline (Second Iteration)
val start = System.currentTimeMillis()
val model = cv.fit(trainingData)
val time = (System.currentTimeMillis() - start) / 1000.0
println(f"Cross-validation training took $time s")

// Evaluate - on beset model
val best_choice_model = model.bestModel.asInstanceOf[PipelineModel]
val predictions = best_choice_model.transform(testData)
predictions.select("sensor_id","temperature","humidity","pressure","status","label","prediction","probability").show(10)

val binaryEval = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
val auc = binaryEval.evaluate(predictions)
val multiEval = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
val accuracy = multiEval.setMetricName("accuracy").evaluate(predictions)
val f1 = multiEval.setMetricName("f1").evaluate(predictions)
println(f"AUC: $auc%.4f, Accuracy: $accuracy%.4f, F1: $f1%.4f")

// Save metrics
val metrics = Seq(
  (System.currentTimeMillis(), trainingData.count(), testData.count(), time, auc, accuracy, f1)
).toDF("timestamp","trainCount","testCount","trainTime_s","auc","accuracy","f1")
val metricsPath = s"$modelDir/metrics.txt"
metrics.coalesce(1).write.mode("overwrite").option("header",true).csv(metricsPath)

// Save model
println(s"Saving model to $modelPath")
best_choice_model.write.overwrite().save(modelPath)

spark.stop()
System.exit(0)