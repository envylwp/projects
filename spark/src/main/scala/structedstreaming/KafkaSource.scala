package structedstreaming

import org.apache.spark.sql.SparkSession

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.1.50.122:9092,10.1.50.123:9092,10.1.50.124:9092")
      .option("subscribe", "lancer_test2_topic")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .option("checkpointLocation","/mnt/disk/data/checkpoint/")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
