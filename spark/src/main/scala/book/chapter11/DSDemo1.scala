package book.chapter11

import org.apache.spark.sql.SparkSession

object DSDemo1 {

  case class Flight(
                     DEST_COUNTRY_NAME:String,
                     ORIGIN_COUNTRY_NAME:String,
                     count:BigInt){
    override def toString: String = s"$DEST_COUNTRY_NAME, $ORIGIN_COUNTRY_NAME, $count"
  }
    case class FlightMetadata(count:BigInt,randomData:BigInt)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()


    import  spark.implicits._
    val flightsDF=spark.read
      .parquet("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/parquet/2010-summary.parquet/")
    val flights=flightsDF.as[Flight]
    flights.show(2)

    def originIsDestination(flight_row:Flight):Boolean={
      return flight_row.ORIGIN_COUNTRY_NAME==flight_row.DEST_COUNTRY_NAME
    }
    println("------------------------------filter------------------------------------")

    println(flights.filter(flight_row=>originIsDestination(flight_row)).first())

    println(flights.collect().filter(flight_row=>originIsDestination(flight_row)))


    println("------------------------------Mapping------------------------------------")

      val destinations = flights.map(f=>f.DEST_COUNTRY_NAME)
      val localDestinations = destinations.take(5).mkString(" | ")
      println(localDestinations)

    println("------------------------------Join------------------------------------")

    val flightsMeta=spark.range(500).map(x=>(x,scala.util.Random.nextLong))
        .withColumnRenamed("_1","count").withColumnRenamed("_2","randomData").as[FlightMetadata]
    val flights2=flights
      .joinWith(flightsMeta,flights.col("count")===flightsMeta.col("count")).show(2,false)

    //不知道为什么不可以
    //flights2.expr("_1.DEST_COUNTRY_NAME")
    //flights2.take(2)

    val flights3=flights.join(flightsMeta,Seq("count")).show(2)
    val flights4=flights.join(flightsMeta.toDF(),Seq("count"))

    println("------------------------------Grouping and Aggregations------------------------------------")

    flights.groupBy("DEST_COUNTRY_NAME").count().show(2)
    flights.groupByKey(x=>x.DEST_COUNTRY_NAME).count().show(2)
    flights.groupByKey(x=>x.DEST_COUNTRY_NAME).count().explain

    def grpSum(countryName:String,values:Iterator[Flight])={
      values.dropWhile(_.count<5).map(x=>(countryName,x))
    }
    flights.groupByKey(x=>x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)

    def grpSum2(f:Flight):Integer={
      1
    }
    println(flights.groupByKey(x=>x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5).mkString("\n"))

    println("------------------------------------------------------------------")
    def sum2(left:Flight,right:Flight)={
      Flight(left.DEST_COUNTRY_NAME,null,left.count+right.count)
    }
    println(flights.groupByKey(x=>x.DEST_COUNTRY_NAME).reduceGroups((l,r)=>sum2(l,r))
      .take(5).mkString("\n"))

    flights.groupBy("DEST_COUNTRY_NAME").count().explain




  }

}
