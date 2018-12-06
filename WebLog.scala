// All imports and initial global declarations
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{
  RandomForestRegressionModel,
  RandomForestRegressor
}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

Object WebLog extends Serializable {

  val rf = new RandomForestRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")

  // Define UDF's that will be used
  val convertUDF = udf((array: Seq[Double]) => {
    Vectors.dense(array.toArray.map(_.toDouble))
  })

  def toTimeStamp(str: String): Timestamp = {
    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm");
    new Timestamp(format.parse(str).getTime)
  }
  val toTime = spark.udf.register("toTime", toTimeStamp _)
  def addValue =
    udf((array: Seq[Double], value: String) =>
      Array(value.replace(".", "").toDouble) ++ array)

  def fixRpsValue =
    udf((array: Seq[Double], value: Double) => {
      val b = array.toBuffer
      b.remove(0)
      val a = b.toArray
      a ++ Array(value)
    })

  def fixSessValue =
    udf((array: Seq[Double], value: Double) => {
      val b = array.toBuffer
      b.remove(1)
      val a = b.toArray
      a ++ Array(value)
    })

  def main(args: Array[String]) {
    if (args.length == 0) {
      println("Required Args </path/to/log/file>")
      System.exit(-1)
    }
    val logfile = args(0)
    val IP = args(1)
    val df = ReadLog(logfile)
    val cleanedDf = CleanData(df)
    println(s"Analytics Challenge-1 ==> Sessionize by IP:")
    val sessionsDf = Sessionize(cleanedDf)
    sessionsDf
      .select("session_id", "clientIp", "session_start", "session_end")
      .show(10)
    val avg_session_length = sessionsDf
      .groupBy(lit(1))
      .agg(avg("session_length").alias("avg_session_length"))
      .select("avg_session_length")
      .collect()(0)(0)
    println(
      s"Analytics Challenge-2 ==> Average Session Length: $avg_session_length (secs)")
    println(s"Analytics Challenge-3 ==> Unique URL visits by Session:")
    sessionsDf.select("session_id", "clientIp", "distinct_urls").show(10)
    println(s"Analytics Challenge-4 ==> Most Engaged IPs:")
    sessionsDf
      .orderBy(desc("session_length"))
      .select($"clientIp", $"session_length".alias("session_length (secs)"))
      .show(10)
    val rps_df = RpsData(cleanedDf)
    val rps_model = TrainRpsModel(rps_df)
    val rps_prediction = PredictRps(rps_df, rps_model)
    println(
      s"Machine Learning Challenge-1 ==> Requests/sec in the next Minute: ${rps_prediction.collect()(0)(0).asInstanceOf[Double].toInt}")
    val (sess_series, sess_length_model) = TrainSessLengthModel(sessionsDf)
    val sess_length_prediction =
      PredictSessLength(sess_series, sess_length_model, "103.24.23.10")
    println(
      s"Machine Learning Challenge-2 ==> Session Length for a given IP (including previous sessions):")
    sess_length_prediction.show()
    val (sess_url_series, sess_url_model) = TrainSessUrlModel(sessionsDf)
    val sess_url_prediction =
      PredictSessUrl(sess_url_series, sess_url_model, "103.24.23.10")
    println(
      s"Machine Learning Challenge-3 ==> Unique Urls for a given IP (including previous sessions):")
    sess_url_prediction.show()
  }

  def ReadLog(file: String): DataFrame = {
    // Read the data
    val customSchema = StructType(
      Array(
        StructField("timestamp", TimestampType, true),
        StructField("elb", StringType, true),
        StructField("clientIpAddress", StringType, true),
        StructField("backendIpAddress", StringType, true),
        StructField("requestProcessingTime", StringType, true),
        StructField("backendProcessingTime", StringType, true),
        StructField("responseProcessingTime", StringType, true),
        StructField("elbStatusCode", StringType, true),
        StructField("backendStatusCode", StringType, true),
        StructField("receivedBytes", StringType, true),
        StructField("sentBytes", StringType, true),
        StructField("request", StringType, true),
        StructField("userAgent", StringType, true),
        StructField("sslCipher", StringType, true),
        StructField("sslProtocol", StringType, true)
      ))
    val df = spark.read
      .format("csv")
      .option("delimiter", " ")
      .option("quote", "\"")
      .schema(customSchema)
      .load(file)
    df
  }

  def CleanData(df: DataFrame): DataFrame = {
    // Clean the data to remove port from the IP and "method" from the requestUrl
    val clean1 = df
      .withColumn("_tmp", split($"clientIpAddress", "\\:"))
      .withColumn("clientIp", $"_tmp".getItem(0))
      .drop("_tmp")
    val clean2 = clean1
      .withColumn("_tmp", split($"request", " "))
      .withColumn("requestUrl", $"_tmp".getItem(1))
      .drop("_tmp")
    val clean3 = clean2.withColumn("dateTime", unix_timestamp($"timestamp"))
    val clean4 =
      clean3.withColumn("minutes", substring($"timestamp".cast("string"), 0, 16))
    clean4.select("timestamp", "clientIp", "requestUrl", "dateTime", "minutes")
  }

  def Sessionize(df: DataFrame): DataFrame = {
    // Create sessionsDf (sessionization by IP using 15 minute interval)
    val sessions = df
      .withColumn(
        "new_session",
        when(($"dateTime" - lag($"dateTime", 1).over(
               Window.partitionBy($"clientIp").orderBy($"dateTime"))) / 60 > 15,
             lit(1)).otherwise(lit(0)))
      .withColumn(
        "session_id",
        concat($"clientIp",
               concat(lit("_"),
                      sum("new_session").over(
                        Window.partitionBy($"clientIp").orderBy($"dateTime")))))
    // Create aggregate sessions dataframe
    val sessionsDf = sessions
      .groupBy("session_id")
      .agg(
        first("clientIp").alias("clientIp"),
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end"),
        min("dateTime").alias("start_datetime"),
        max("dateTime").alias("end_datetime"),
        countDistinct("requestUrl").alias("distinct_urls")
      )
      .withColumn("session_length",
                  ($"end_datetime" - $"start_datetime").cast("double"))
    sessionsDf.cache.count
    sessionsDf
  }

  // Get the Requests-per-second (rps) data
  def RpsData(df: DataFrame): DataFrame = {
    val rpsDf = df
      .groupBy($"minutes")
      .agg(count(lit(1)).alias("rps"))
      .withColumn("rps", $"rps" / 60)
      .withColumn("timestamp", toTime($"minutes").cast("double"))
      .select("timestamp", "rps", "minutes")
    // Convert the rps data into sequences for time series type processing
    val rpsGrp = rpsDf
      .orderBy("minutes")
      .withColumn(
        "collector",
        collect_list("rps").over(
          Window.partitionBy(lit(1)).orderBy("minutes").rowsBetween(-5, -1)))
      .withColumn(
        "collect_counts",
        count(lit(1)).over(
          Window.partitionBy(lit(1)).orderBy("minutes").rowsBetween(-5, -1)))
      .filter($"collect_counts" === 5)
    rpsGrp.cache.count
    rpsGrp
  }

  def TrainRpsModel(df: DataFrame): RandomForestRegressionModel = {
    val rpsSeries
      : org.apache.spark.sql.Dataset[org.apache.spark.ml.feature.LabeledPoint] =
      df.withColumn("features", convertUDF($"collector"))
        .withColumnRenamed("rps", "label")
        .select("label", "features")
        .as[org.apache.spark.ml.feature.LabeledPoint]
    val rps_model = rf.fit(rpsSeries)
    rps_model
  }

  def PredictRps(rps_df: DataFrame,
                 model: RandomForestRegressionModel): DataFrame = {
    val rpsPredCollector = rps_df
      .orderBy(desc("timestamp"))
      .limit(1)
      .withColumn("collector", fixRpsValue($"collector", $"rps"))
      .select("collector")
      .take(1)(0)(0)
      .asInstanceOf[Seq[Double]]
    val newDf
      : org.apache.spark.sql.Dataset[org.apache.spark.ml.feature.LabeledPoint] =
      Seq((rpsPredCollector, 1.0))
        .toDF("features", "label")
        .withColumn("features", convertUDF($"features"))
        .select("label", "features")
        .as[org.apache.spark.ml.feature.LabeledPoint]
    val predictions = model.transform(newDf)
    predictions.select($"prediction".alias("rps"))
  }

  def TrainSessLengthModel(
      sessionsDf: DataFrame): (DataFrame, RandomForestRegressionModel) = {
    val sessionSeries = sessionsDf
      .orderBy("clientIp", "session_start")
      .withColumn("collector",
                  collect_list("session_length").over(
                    Window
                      .partitionBy("clientIp")
                      .orderBy("session_start")
                      .rowsBetween(-3, -1)))
      .withColumn("collect_counts",
                  count(lit(1)).over(
                    Window
                      .partitionBy("clientIp")
                      .orderBy("session_start")
                      .rowsBetween(-3, -1)))
      .withColumn("collector", addValue($"collector", $"clientIp"))
      .filter($"collect_counts" === 3)
    sessionSeries.cache.count
    val sessionSeriesData
      : org.apache.spark.sql.Dataset[org.apache.spark.ml.feature.LabeledPoint] =
      sessionSeries
        .withColumn("features", convertUDF($"collector"))
        .withColumnRenamed("session_length", "label")
        .select("label", "features")
        .as[org.apache.spark.ml.feature.LabeledPoint]
    val session_model = rf.fit(sessionSeriesData)
    (sessionSeries, session_model)
  }

  def PredictSessLength(df: DataFrame,
                        model: RandomForestRegressionModel,
                        IP: String): DataFrame = {
    val test = df
      .filter($"clientIp" === IP)
      .orderBy(desc("session_start"))
      .withColumn("collector", fixSessValue($"collector", $"distinct_urls"))
      .select("collector")
      .take(1)(0)(0)
      .asInstanceOf[Seq[Double]]
    val testDf
      : org.apache.spark.sql.Dataset[org.apache.spark.ml.feature.LabeledPoint] =
      Seq((test, 1.0))
        .toDF("features", "label")
        .withColumn("features", convertUDF($"features"))
        .select("label", "features")
        .as[org.apache.spark.ml.feature.LabeledPoint]
    val session_predictions =
      model.transform(testDf).withColumn("prediction", $"prediction".cast("int"))
    df.filter($"clientIp" === IP)
      .select(lit("existing").alias("session"), $"session_length")
      .union(session_predictions.select(lit("predicted").alias("session"),
                                        $"prediction".alias("session_length")))
  }

  def TrainSessUrlModel(
      sessionsDf: DataFrame): (DataFrame, RandomForestRegressionModel) = {
    val sessionUrlSeries = sessionsDf
      .orderBy("clientIp", "session_start")
      .withColumn("distinct_urls", $"distinct_urls".cast("double"))
      .withColumn("collector",
                  collect_list("distinct_urls").over(
                    Window
                      .partitionBy("clientIp")
                      .orderBy("session_start")
                      .rowsBetween(-3, -1)))
      .withColumn("collect_counts",
                  count(lit(1)).over(
                    Window
                      .partitionBy("clientIp")
                      .orderBy("session_start")
                      .rowsBetween(-3, -1)))
      .withColumn("collector", addValue($"collector", $"clientIp"))
      .filter($"collect_counts" === 3)
    sessionUrlSeries.cache.count
    val sessionUrlSeriesData
      : org.apache.spark.sql.Dataset[org.apache.spark.ml.feature.LabeledPoint] =
      sessionUrlSeries
        .withColumn("features", convertUDF($"collector"))
        .withColumnRenamed("distinct_urls", "label")
        .select("label", "features")
        .as[org.apache.spark.ml.feature.LabeledPoint]
    val session_url_model = rf.fit(sessionUrlSeriesData)
    (sessionUrlSeries, session_url_model)
  }

  def PredictSessUrl(df: DataFrame,
                     model: RandomForestRegressionModel,
                     IP: String): DataFrame = {
    val test = df
      .filter($"clientIp" === IP)
      .orderBy(desc("session_start"))
      .withColumn("collector", fixSessValue($"collector", $"distinct_urls"))
      .select("collector")
      .take(1)(0)(0)
      .asInstanceOf[Seq[Double]]
    val testDf
      : org.apache.spark.sql.Dataset[org.apache.spark.ml.feature.LabeledPoint] =
      Seq((test, 1.0))
        .toDF("features", "label")
        .withColumn("features", convertUDF($"features"))
        .select("label", "features")
        .as[org.apache.spark.ml.feature.LabeledPoint]
    val session_predictions =
      model.transform(testDf).withColumn("prediction", $"prediction".cast("int"))
    df.filter($"clientIp" === IP)
      .select(lit("existing").alias("session"), $"distinct_urls")
      .union(session_predictions.select(lit("predicted").alias("session"),
                                        $"prediction".alias("distinct_urls")))
  }

}