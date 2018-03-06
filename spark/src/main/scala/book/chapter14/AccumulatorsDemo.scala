package book.chapter14

import org.apache.spark.sql.SparkSession

object AccumulatorsDemo {

  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String, count: BigInt)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    import spark.implicits._
    val flights = spark.read
      .parquet("/data/flight-data/parquet/2010-summary.parquet")
      .as[Flight]
    import org.apache.spark.util.LongAccumulator
    val accUnnamed = new LongAccumulator
    val acc = spark.sparkContext.register(accUnnamed)
    val accChina = new LongAccumulator
    val accChina2 = spark.sparkContext.longAccumulator("China")
    spark.sparkContext.register(accChina, "China")

    def accChinaFunc(flight_row: Flight) = {
      val destination = flight_row.DEST_COUNTRY_NAME
      val origin = flight_row.ORIGIN_COUNTRY_NAME
      if (destination == "China") {
        accChina.add(flight_row.count.toLong)
      }
      if (origin == "China") {
        accChina.add(flight_row.count.toLong)
      }
    }

    flights.foreach(flight_row => accChinaFunc(flight_row))
  }
}
