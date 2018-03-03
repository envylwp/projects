package book.chapter12

import org.apache.spark.sql.SparkSession

object RDD {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder
        .appName("StructuredNetworkWordCount")
        .master("local")
        .config("spark.driver.bindAddress", "20000")
        .getOrCreate()

    spark.range(500).rdd
    spark.range(10).toDF().rdd.map(rowObject=>rowObject.getLong(0))
    import spark.implicits._
    spark.range(10).rdd.toDF()

    val words=spark.sparkContext.parallelize(myCollection,2)
  }

}
