package book.chapter6

import org.apache.spark.sql.SparkSession

object UDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val udfExampleDF=spark.range(5).toDF("num")
    def power3(number:Double):Double=number*number*number
    power3(2.0)
    import org.apache.spark.sql.functions.{udf, col}
    val power3udf=udf(power3(_:Double):Double)
    udfExampleDF.select(power3udf(col("num"))).show()

    spark.udf.register("power3",power3(_:Double):Double)
    udfExampleDF.selectExpr("power3(num)").show(2)
  }

}
