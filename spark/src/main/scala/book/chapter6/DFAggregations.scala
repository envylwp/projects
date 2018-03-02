package book.chapter6

import org.apache.spark.sql.SparkSession

object DFAggregations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/retail-data/all/*.csv")
      .coalesce(5)
    df.printSchema()
    df.cache()
    df.createOrReplaceTempView("dfTable")
    //df.show(2)
    //println(df.count())


    println("------------------------------count------------------------------------")
//    import org.apache.spark.sql.functions.count
//    df.select(count("StockCode")).show()// 541909
//    spark.sql("""SELECT COUNT(*) FROM dfTable""").show()

    println("------------------------------countDistinct------------------------------------")
//
//    import org.apache.spark.sql.functions.countDistinct
//    df.select(countDistinct("StockCode")).show()// 4070
//    spark.sql("""SELECT COUNT(DISTINCT *) FROM DFTABLE""").show()


    println("------------------------------approx_count_distinct------------------------------------")
//    import org.apache.spark.sql.functions.approx_count_distinct
//    df.select(approx_count_distinct("StockCode",0.1)).show()// 3364
//    spark.sql("""SELECT approx_count_distinct(StockCode,0.1) FROM DFTABLE""").show()



    println("------------------------------first and last------------------------------------")
//    import org.apache.spark.sql.functions.{first,last}
//    df.select(first("StockCode"),last("StockCode")).show()
//    spark.sql("""SELECT first(StockCode),last(StockCode) FROM dfTable""").show()

    println("------------------------------min and max------------------------------------")
//    import org.apache.spark.sql.functions.{min,max}
//    df.select(min("Quantity"),max("Quantity")).show(2)
//    spark.sql("""SELECT min(Quantity),max(Quantity) FROM dfTable""").show()



    println("------------------------------sum------------------------------------")
    import org.apache.spark.sql.functions.sum
    df.select(sum("Quantity")).show()// 5176450
    spark.sql("""SELECT sum(Quantity) FROM dfTable""").show()


    println("------------------------------sumDistinct------------------------------------")
    import org.apache.spark.sql.functions.sumDistinct
    df.select(sumDistinct("Quantity")).show()// 29310
//    spark.sql("""SELECT sumDistinct(Quantity) FROM dfTable""").show()


    println("------------------------------avg------------------------------------")















  }




}
