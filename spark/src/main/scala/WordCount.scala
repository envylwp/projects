import org.apache.spark.sql.SparkSession

object WordCount {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
        .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    import spark.implicits._
    val value = spark.sparkContext.textFile("src/main/resources/wc.txt")
      .map(_.split(" "))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    value.show()
    value.groupBy("name").count().show()


    spark.stop()


  }
}
