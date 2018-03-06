package book.chapter19

import org.apache.spark.sql.SparkSession

object StructedStreamingDemo4Join {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()
    /**
      * {
      * "Arrival_Time":1424686735982,
      * "Creation_Time":1424688582035859500,
      * "Device":"nexus4_2",
      * "Index":203,
      * "Model":"nexus4",
      * "User":"g",
      * "gt":"stand",
      * "x":0.0017547607,
      * "y":-0.018981934,
      * "z":-0.022201538
      * }
      */

    val dataPath = "/mnt/disk/IdeaProjects/projects/spark/data/activity-data/"
    val static = spark.read.json(dataPath)
    val dataSchema = static.schema
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    val streaming = spark.readStream.schema(dataSchema)
      .option("maxFilesPerTrigger", 1).json(dataPath)

    val historicalAgg = static.groupBy("gt", "model").avg()
    val deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")
      .cube("gt", "model").avg()
      .join(historicalAgg, Seq("gt", "model"))
      .writeStream.queryName("device_counts").format("memory").outputMode("complete")
      .start()

    for (i <- 1 to 50) {

      println("------------------------------------------------------------------")

      spark.sql("SELECT * FROM device_counts").show()
      Thread.sleep(1000)
    }

    deviceModelStats.awaitTermination()

  }
}
