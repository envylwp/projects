package book.chapter19

import org.apache.spark.sql.SparkSession

object SparkSqlQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    for (i <- 1 to 5) {
      spark.sql("SELECT * FROM activity_counts").show()
      Thread.sleep(1000)
    }

  }

}
