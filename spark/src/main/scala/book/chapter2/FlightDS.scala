package book.chapter2

import org.apache.spark.sql.SparkSession

object FlightDS {

    case class Flight(DEST_COUNTRY_NAME:String,
      ORIGIN_COUNTRY_NAME:String,
      count:BigInt)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val flightsDF = spark.read
      .parquet("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/parquet/2010-summary.parquet/")

    //ds 必须要有这句
    import  spark.implicits._
    val flights = flightsDF.as[Flight]
    flights.show(10)

    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)

    flights
      .take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr => Flight(fr.DEST_COUNTRY_NAME,fr.ORIGIN_COUNTRY_NAME,fr.count+5))

    println("+++++++++++++++++++++++++")
    val staticDataFrame = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/retail-data/by-day/*.csv")

    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema
    println(staticSchema)

    val frame = spark.sql("""
                         select CustomerId, UnitPrice, InvoiceDate, Quantity
                         from retail_data
                          """)
    frame.show(10)

    import org.apache.spark.sql.functions.{window,column,desc,col}
    staticDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"),window(col("InvoiceDate"),"1 day"))
      .sum("total_cost")
      .show(20)



  }
}
