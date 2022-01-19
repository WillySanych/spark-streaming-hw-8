import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt

// https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
object SparkApp extends App {

  val spark = SparkSession.builder
    .appName("spark-streaming-hw")
    .master(sys.env.getOrElse("spark.master", "local[*]"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val brokers = "localhost:9092"
  val topic = "records"

  val records: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()

  import spark.implicits._

  val scheme = new StructType()
    .add("sensor1", "double")
    .add("sensor2", "double")
    .add("sensor3", "double")
    .add("sensor4", "double")
    .add("sensor5", "double")

  val DataFrame = records.withColumn("sensData", from_json(col("value").cast("String"),scheme))
    .select("timestamp", "sensData.*")
    .groupBy(window($"timestamp", "20 seconds", "20 seconds"))
    .avg("sensor1", "sensor2", "sensor3", "sensor4", "sensor5")

  val query = DataFrame.writeStream
    .outputMode("update")
    .format("console")
    .start()

  query.awaitTermination()

}
