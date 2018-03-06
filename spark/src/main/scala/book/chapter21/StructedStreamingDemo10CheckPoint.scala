package book.chapter21

import org.apache.spark.sql.SparkSession

object StructedStreamingDemo10CheckPoint {

  case class InputRow(user: String, timestamp: java.sql.Timestamp, activity: String)

  case class UserState(user: String,
                       var activity: String,
                       var start: java.sql.Timestamp,
                       var end: java.sql.Timestamp)


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
      .count()



//    val query = streaming
//      .writeStream
//      .outputMode("complete")
//      .option("checkpointLocation", "/some/location/")
//      .queryName("test_stream")
//      .format("memory")
//      .start()


  }
}
