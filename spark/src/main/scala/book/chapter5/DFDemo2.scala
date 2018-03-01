package book.chapter5

import org.apache.spark.sql.SparkSession

object DFDemo2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val df = spark.read.format("json")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/json/2015-summary.json")




  }

}
