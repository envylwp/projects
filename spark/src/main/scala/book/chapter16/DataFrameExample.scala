package book.chapter16

import org.apache.spark.sql.SparkSession

object DataFrameExample extends Serializable {
  def main(args: Array[String]) = {

    val pathToDataFolder = args(0)

    // start up the SparkSession
    // along with explicitly setting a given config
    val spark = SparkSession.builder().appName("Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    // udf registration
    import spark.implicits._
//    spark.udf.register("myUDF", someUDF(_:String):String)
//    val df = spark.read.json(pathToDataFolder + "data.json")
//    val manipulated = df.groupBy(expr("myUDF(group)")).sum().collect()
//      .foreach(println(_))

  }
}
