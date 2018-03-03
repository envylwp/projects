package book.chapter8

import org.apache.spark.sql.SparkSession

object ReadingData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    println("------------------------------read------------------------------------")
    spark.read.format("csv")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .option("path", "path/to/file(s)")
      .schema(null)
      .load()


    println("------------------------------write------------------------------------")

//
//
//      DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(
//      ...).save()
//
//    dataframe.write.format("csv")
//      .option("mode","OVERWRITE")
//      .option("dateFormat","yyyy-MM-dd")
//      .option("path","path/to/file(s)")
//      .save()




  }

}
