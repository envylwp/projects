package book.chapter20

import org.apache.spark.sql.SparkSession

object StructedStreamingDemo8WaterMarking {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 5)
    val static = spark.read.json("/mnt/disk/IdeaProjects/projects/spark/data/activity-data")
    val streaming = spark
      .readStream
      .schema(static.schema)
      .option("maxFilesPerTrigger", 10)
      .json("/mnt/disk/IdeaProjects/projects/spark/data/activity-data")

    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    import org.apache.spark.sql.functions.{window, col}
    val query = withEventTime
      .withWatermark("event_time", "5 hours")
      .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
      .count()
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("complete")
      .start()

    for (i <- 1 to 50) {

      println("------------------------------------------------------------------")

      spark.sql("SELECT * FROM events_per_window").show(false)
      Thread.sleep(1000)
    }


    import org.apache.spark.sql.functions.expr
    withEventTime
      .withWatermark("event_time","5 seconds")
      .dropDuplicates("User","event_time")
      .groupBy("User")
      .count()
      .writeStream
      .queryName("deduplicated")
      .format("memory")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}
