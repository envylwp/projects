package book.chapter2

import org.apache.spark.sql.SparkSession

object FlightDFSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()
    //default 200
    spark.conf.set("spark.sql.shuffle.partitions","5")

    val flightData2015 = spark
      .read
      .option("inferSchema","true")
      .option("header","true")
      .csv("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/csv/2015-summary.csv")


    //show Physical Plan
    //flightData2015.sort("count").explain()
    /*
    == Physical Plan ==
    *Sort [count#14 ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(count#14 ASC NULLS FIRST, 200)
      +- *FileScan csv [DEST_COUNTRY_NAME#12,ORIGIN_COUNTRY_NAME#13,count#14] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/mnt/disk/IdeaProjects/projects/spark/data/flight-data/csv/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:int>
    */

    flightData2015.sort("count").show(2)

    flightData2015.createOrReplaceTempView("flight_data_2015")
    val sqlWay = spark.sql("""
          SELECT DEST_COUNTRY_NAME, count(1)
          FROM flight_data_2015
          GROUP BY DEST_COUNTRY_NAME
          """)
    val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
    //下面两种情况等价
    //sqlWay.explain
    //dataFrameWay.explain

    //下面两种情况等价
    spark.sql("SELECT max(count) from flight_data_2015").take(1).foreach(println(_))
    import org.apache.spark.sql.functions.max
    flightData2015.select(max("count")).take(1).foreach(println(_))


    val maxSql=spark.sql("""
          SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
          FROM flight_data_2015
          GROUP BY DEST_COUNTRY_NAME
          ORDER BY sum(count) DESC
          LIMIT 5
          """)
    maxSql.show()

    import org.apache.spark.sql.functions.desc
    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)","destination_total")
      .orderBy(desc("destination_total"))
      .limit(5)
      .show()

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)","destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()


    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)","destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .explain()
    /*
    == Physical Plan ==
    TakeOrderedAndProject(limit=5, orderBy=[destination_total#108L DESC NULLS LAST], output=[DEST_COUNTRY_NAME#12,destination_total#108L])
    +- *HashAggregate(keys=[DEST_COUNTRY_NAME#12], functions=[sum(cast(count#14 as bigint))])
       +- Exchange hashpartitioning(DEST_COUNTRY_NAME#12, 5)
          +- *HashAggregate(keys=[DEST_COUNTRY_NAME#12], functions=[partial_sum(cast(count#14 as bigint))])
             +- *FileScan csv [DEST_COUNTRY_NAME#12,count#14] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/mnt/disk/IdeaProjects/projects/spark/data/flight-data/csv/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,count:int>
     */

  }

}
