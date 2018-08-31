package book.chapter10

import org.apache.spark.sql.SparkSession

object SparkSQL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20001")
      .getOrCreate()

    spark.read.json("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/json/2015-summary.json")
    //      .createOrReplaceTempView("some_sql_view") // DF =>SQL
//    spark.sparkContext.textFile("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/json/2015-summary.json")
    println("--------------------")
    //    spark.catalog.cacheTable("some_sql_view")

    //    spark.sql(
    //      """
    //        |select * from some_sql_view
    //        |
    //      """.stripMargin).show(10)
    Thread.sleep(20000)
    //    spark.catalog.uncacheTable("some_sql_view")
    Thread.sleep(Integer.MAX_VALUE)

    //        spark.sql("""
    //                SELECT DEST_COUNTRY_NAME, sum(count)
    //                FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
    //                """)
    //          .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` >10")
    //          .count()// SQL =>DF
  }
}
