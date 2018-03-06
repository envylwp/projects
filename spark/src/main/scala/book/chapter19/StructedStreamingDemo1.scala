package book.chapter19

import org.apache.spark.sql.SparkSession

object StructedStreamingDemo1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val dataPath = "/mnt/disk/IdeaProjects/projects/spark/data/activity-data/"
    val static = spark.read.json(dataPath)
    val dataSchema = static.schema
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    //println(dataSchema)
    val streaming = spark.readStream.schema(dataSchema)
      .option("maxFilesPerTrigger", 1).json(dataPath)

    val activityCounts = streaming.groupBy("gt").count()
    val activityQuery = activityCounts.writeStream.queryName("activity_counts")
      .format("memory").outputMode("complete")
      .start()

    for (i <- 1 to 50) {

      println("------------------------------------------------------------------")

      spark.sql("SELECT * FROM activity_counts").show()
      Thread.sleep(1000)
    }

    activityQuery.awaitTermination()

  }
}
