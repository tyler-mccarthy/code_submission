import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.commons.math3.distribution.TDistribution

// initialising spark session and config
val spark = SparkSession.builder().appName("AmazonHypothesisAnalysis").config("spark.master", "local[*]").getOrCreate()

// define schema from task definition
// * userId: Every user identified with a unique id (First Column)
// * productId: Every product identified with a unique id(Second Column)
// * Rating : Rating of the corresponding product by the corresponding user(Third Column)
// * timestamp : Unix Time of the rating ( Fourth Column)
val schema = StructType(Array(
    StructField("userId", StringType, nullable = true),
    StructField("productId", StringType, nullable = true),
    StructField("Rating", DoubleType, nullable = true),
    StructField("timestamp", LongType, nullable = true),
))

// load the csv with schema
val df = spark.read.option("header", "false")
    .schema(schema).csv("file:///home/bd_admin/big-data-task/data/ratings_electronics.csv")

val df_with_readable_dates = df.withColumn("date", from_unixtime(col("timestamp")).cast("date"))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))

val q2_2014 = df_with_readable_dates.filter(col("year") === 2014 && col("month").between(4, 6))
val q2_count = q2_2014.count()
println(f"Total number of rows in dataset for Q2: $q2_count")

// split the data further into above and below rating of 3
val above_3 = q2_2014.filter(col("Rating") > 3)
val below_3 = q2_2014.filter(col("Rating") < 3)

// run analysis on each of these groups across ratings, reviews, products
def computeStats(df: org.apache.spark.sql.DataFrame): Row = {
    df.agg(
        avg("Rating").alias("avg_rating"),
        variance("Rating").alias("variance"),
        count("*").alias("num_reviews"),
        countDistinct("productId").alias("unique_products")
    ).first()
}

// calculate stats for ratings above 3
val above3Stats = computeStats(above_3)

val below3Stats = computeStats(below_3)

// def get_t_test_vals(row: Row): (Double, Double, Double, Long) = {
//     val mean = row.getAs[Double]("mean")
//     val variance = row.getAs[Double]("variance")
//     val n = row.getAs[Long]("n_unique").toDouble
//     val uniqueProd = row.getAs[Long]("unique_products")
//     (mean, variance, n, uniqueProd)
// }


// get the values from each of the datasets (only containing one row)
val mean1 = above3Stats.getAs[Double]("avg_rating")
val variance1 = above3Stats.getAs[Double]("variance")
val n1 = above3Stats.getAs[Long]("num_reviews").toDouble
val uniqueProd1 = above3Stats.getAs[Long]("unique_products")

// unpack values
val mean2 = below3Stats.getAs[Double]("avg_rating")
val variance2 = below3Stats.getAs[Double]("variance")
val n2 = below3Stats.getAs[Long]("num_reviews").toDouble
val uniqueProd2 = below3Stats.getAs[Long]("unique_products")

println(s"Above 3: mean=$mean1, variance=$variance1, n=$n1, unique_prods=$uniqueProd1")
println(s"Above 3: mean=$mean2, variance=$variance2, n=$n2, unique_prods=$uniqueProd2")

def welchTTest(
    mean1: Double, var1: Double, n1: Double,
    mean2: Double, var2: Double, n2: Double
): (Double, Double, Double) = {
    val se1 = var1 / n1
    val se2 = var2 / n2

    // welch t-statistic
    val tStat = (mean1 - mean2) / math.sqrt(se1 + se2)
    // welch-satterthwaite
    val numerator = math.pow(se1 + se2, 2)
    val denominator = math.pow(se1, 2) / (n1 - 1) + math.pow(se2, 2) / (n2 - 1)
    val dfwelch = numerator / denominator

    // two-tailed p-value
    val td = new TDistribution(dfwelch)
    val pValue = 2.0 * (1.0 - td.cumulativeProbability(math.abs(tStat)))

    (tStat, dfwelch, pValue) 
}


// plug these into the 
val (tStat, dF, p) = welchTTest(mean1, variance1, n1, mean2, variance2, n2)

println(f"Welch t = $tStat, df = $dF, p-val = $p")
