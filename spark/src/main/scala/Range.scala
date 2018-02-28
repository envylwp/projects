import org.apache.spark.sql.SparkSession

object Range {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()


    val frame = spark.range(1000).toDF("aaa")
    frame.show(10)

  }
}
