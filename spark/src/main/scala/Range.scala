import org.apache.spark.sql.SparkSession

object Range {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20001")
      .getOrCreate()


    val frame = spark.range(1000).toDF("aaa")
    frame.createOrReplaceTempView("person")
    spark.sql("select * from person limit 1").show()

    spark.catalog.listColumns("person").show()
    val columns = spark.catalog.listColumns("person").collect()
    columns.foreach(c => println(c.toString()))
    val buffer = columns.toBuffer
    println(buffer)

  }
}
