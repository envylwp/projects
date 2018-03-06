package book.chapter18

import org.apache.spark.sql.SparkSession

object OnlineRetailDSDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/retail-data/all/online-retail-dataset.csv")
    df.printSchema()
    val rows = df.repartition(2).selectExpr("instr(Description, 'GLASS') >= 1 as is_glass").groupBy("is_glass").count().collect()

    Thread.sleep(Integer.MAX_VALUE)
    spark.stop()


  }

}
