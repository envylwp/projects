package book.chapter4

import org.apache.spark.sql.SparkSession

object DF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val df = spark.range(500).toDF("number")
    df.select(df.col("number") + 10)

    spark.range(2).toDF().collect()

    import org.apache.spark.sql.types._
    val b = ByteType



  }
}
