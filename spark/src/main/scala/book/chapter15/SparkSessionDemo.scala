package book.chapter15

import org.apache.spark.sql.SparkSession

object SparkSessionDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Databricks Spark Example")
        .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()


    import org.apache.spark.SparkContext
    val sc = SparkContext.getOrCreate()

    println(spark.sparkContext)
  }
}
