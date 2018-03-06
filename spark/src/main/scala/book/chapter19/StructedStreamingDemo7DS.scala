package book.chapter19

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{ForeachWriter, SparkSession}

object StructedStreamingDemo7DS {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)

  def main(args: Array[String]): Unit = {

//    val spark = SparkSession
//      .builder
//      .appName("StructuredNetworkWordCount")
//      .master("local")
//      .config("spark.driver.bindAddress", "20000")
//      .getOrCreate()
//
//    val dataSchema = spark.read
//      .parquet("/data/flight-data/parquet/2010-summary.parquet/")
//      .schema
//    val flightsDF = spark.readStream.schema(dataSchema)
//      .parquet("/data/flight-data/parquet/2010-summary.parquet/")
//    val flights = flightsDF.as[Flight]
//
//    def originIsDestination(flight_row: Flight): Boolean = {
//      return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
//    }
//
//    flights.filter(flight_row => originIsDestination(flight_row))
//      .groupByKey(x => x.DEST_COUNTRY_NAME).count()
//      .writeStream.queryName("device_counts").format("memory").outputMode("complete")
//      .start()


  }
}
