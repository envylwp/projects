package book.chapter6

import breeze.linalg.sum
import org.apache.spark.sql.SparkSession

object DFMath {
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

//    println("------------------------------Working with Strings------------------------------------")
//    import org.apache.spark.sql.functions.{var_pop,stddev_pop}
//    import org.apache.spark.sql.functions.{var_samp,stddev_samp}
//    df.select(var_pop("Quantity"),var_samp("Quantity"),
//      stddev_pop("Quantity"),stddev_samp("Quantity")).show(2)
//
//    spark.sql("""SELECT var_pop(Quantity),var_samp(Quantity), stddev_pop(Quantity),stddev_samp(Quantity)
//                FROM dfTable""").show(2)
//
//    println("------------------------------skewness and kurtosis------------------------------------")
//    import org.apache.spark.sql.functions.{skewness,kurtosis}
//    df.select(skewness("Quantity"),kurtosis("Quantity")).show()
//    spark.sql("SELECT skewness(Quantity),kurtosis(Quantity) FROM dfTable").show()
//
//    println("------------------------------Covariance and Correlation------------------------------------")
//    import org.apache.spark.sql.functions.{corr,covar_pop,covar_samp}
//    df.select(corr("InvoiceNo","Quantity"),covar_samp("InvoiceNo","Quantity"),
//      covar_pop("InvoiceNo","Quantity")).show()
//    spark.sql("""SELECT corr(InvoiceNo,Quantity),covar_samp(InvoiceNo,Quantity),
//            covar_pop(InvoiceNo,Quantity)
//            FROM dfTable""")

    println("------------------------------Aggregating to Complex Types------------------------------------")

//    import org.apache.spark.sql.functions.{collect_set,collect_list}
//    df.agg(collect_set("Country"),collect_list("Country")).show()
//    spark.sql("""SELECT collect_set(Country),collect_set(Country) FROM dfTable""")

    println("------------------------------Grouping------------------------------------")
//    df.groupBy("InvoiceNo","CustomerId").count().show()
//    spark.sql("""SELECT count(*) FROM dfTable GROUP BY InvoiceNo,CustomerId""").show()


    println("------------------------------Grouping with Expressions------------------------------------")

//    import org.apache.spark.sql.functions.{count,expr}
//
//    df.groupBy("InvoiceNo").agg(
//      count("Quantity").alias("quan"),
//      expr("count(Quantity)")).show()

    println("------------------------------Window Functions------------------------------------")
    import org.apache.spark.sql.functions.{col,to_date}
    val dfWithDate=df.withColumn("date",to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    dfWithDate.show(5,false)

    /**
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.col
    val windowSpec=Window
      .partitionBy("CustomerId","date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    import org.apache.spark.sql.functions.max
    val maxPurchaseQuantity=max(col("Quantity")).over(windowSpec)
    println(maxPurchaseQuantity.toString())


    import org.apache.spark.sql.functions.{dense_rank,rank}
    val purchaseDenseRank=dense_rank().over(windowSpec)
    val purchaseRank=rank().over(windowSpec)


    import org.apache.spark.sql.functions.col

    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


      */

    println("------------------------------Grouping Sets------------------------------------")


    println("------------------------------Rollups------------------------------------")

    println("------------------------------Cube------------------------------------")










  }

}
