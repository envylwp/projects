package structedstreaming

import org.apache.spark.sql.SparkSession

object KafkaSourceDemo {
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
      .option("subscribe", "lancer_test_clickstream_topic")
      .load()
      //      .selectExpr("CAST(value AS STRING)")
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as v", "topic", "partition", "offset", "timestamp", "timestampType")
      .as[(String, String, String, String, String, String, String)]
      .createOrReplaceTempView("t_log")

//    val r = spark.sql(
//      """
//        select count(1) as c, v from t_log
//        group by v
//        order by c desc
//      """.stripMargin)

    val r = spark.sql(
      """
        select count(topic) as c, topic from t_log
        group by topic
      """.stripMargin)




    // Generate running word count
    //    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = r.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "/mnt/disk/data/checkpoint/")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
