package book.chapter2

import org.apache.spark.sql.SparkSession

object StructedStreamingDemo1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions","5")

    val staticDataFrame = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/retail-data/by-day/*.csv")

    val staticSchema = staticDataFrame.schema

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger",1)
      .format("csv")
      .option("header","true")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/retail-data/by-day/*.csv")

    //判断是否是streaming
    println(streamingDataFrame.isStreaming)

    import org.apache.spark.sql.functions.{window,column,desc,col}
    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"),window(col("InvoiceDate"),"1 day"))
      .sum("total_cost")


   val query =  purchaseByCustomerPerHour.writeStream
      .format("memory")// memory = store in-memory table
      .queryName("customer_purchases")// the name of the in-memory table
      .outputMode("complete")// complete = all the counts should be in the table
      .start()

    spark.sql("""
              SELECT *
              FROM customer_purchases
              ORDER BY `sum(total_cost)` DESC
              """)
      .show(5)

    //query.awaitTermination()
  }
}
