package book.chapter6

import org.apache.spark.sql.SparkSession

object DFDemo3 {
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
      .load("/mnt/disk/IdeaProjects/projects/spark/data/retail-data/by-day/2010-12-01.csv")
    df.printSchema()
    df.createOrReplaceTempView("dfTable")

    println("------------------------------Converting to Spark Types------------------------------------")

    import org.apache.spark.sql.functions.lit
    df.select(lit(5),lit("five"),lit(5.0)).show(5)
    spark.sql(
      """
        SELECT 5,"five",5.0
      """.stripMargin).show(5)

    println("------------------------------Working with Booleans------------------------------------")
    import org.apache.spark.sql.functions.col
    df.select("InvoiceNo","Description")
      .where(col("InvoiceNo").equalTo(536365))
      .show(5,false)

    import org.apache.spark.sql.functions.col
    df.where(col("InvoiceNo")===536365)
      .select("InvoiceNo","Description")
      .show(5,false)

    df.where("InvoiceNo = 536365")
      .show(5,false)

    val priceFilter=col("UnitPrice")>600
    val descripFilter=col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
      .show()
    spark.sql(
      """
        SELECT *
        FROM dfTable
        WHERE StockCode in("DOT") AND (UnitPrice>600 OR
        instr(Description,"POSTAGE")>=1)

      """.stripMargin).show(5)

    val DOTCodeFilter=col("StockCode")==="DOT"
    df.withColumn("isExpensive",DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice","isExpensive").show(5)

    spark.sql(
      """
            SELECT UnitPrice,(StockCode='DOT'AND
            (UnitPrice>600 OR instr(Description,"POSTAGE")>=1)) as isExpensive
            FROM dfTable
            WHERE (StockCode='DOT'AND
            (UnitPrice>600 OR instr(Description,"POSTAGE")>=1))
      """.stripMargin).show(5)

    import org.apache.spark.sql.functions.{expr,not,col}
    df.withColumn("isExpensive",not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description","UnitPrice").show(5)
    df.withColumn("isExpensive",expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description","UnitPrice").show(5)

    df.where(col("Description").eqNullSafe("hello")).show()



    println("------------------------------Working with Numbers------------------------------------")

    import org.apache.spark.sql.functions.{expr,pow}
    val fabricatedQuantity=pow(col("Quantity")*col("UnitPrice"),2)+5
    df.select(expr("CustomerId"),fabricatedQuantity.alias("realQuantity")).show(2)
    df.selectExpr(
      "CustomerId",
      "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
    spark.sql(
      """
        SELECT customerId,(POWER((Quantity*UnitPrice),2.0)+5) as realQuantity
        FROM dfTable
      """.stripMargin).show(2)

    import org.apache.spark.sql.functions.{round,bround}
    df.select(round(col("UnitPrice"),1).alias("rounded"),col("UnitPrice")).show(5)
    import org.apache.spark.sql.functions.lit
    df.select(round(lit("2.5")),bround(lit("2.5"))).show(2)

    import org.apache.spark.sql.functions.{corr}
    df.stat.corr("Quantity","UnitPrice")
    df.select(corr("Quantity","UnitPrice")).show()
    spark.sql("""SELECT corr(Quantity,UnitPrice) FROM dfTable""").show(2)

    //describe...
    df.describe().show()

    import org.apache.spark.sql.functions.{count,mean,stddev_pop,min,max}
    val colName="UnitPrice"
    val quantileProbs=Array(0.5)
    val relError=0.05
    df.stat.approxQuantile("UnitPrice",quantileProbs,relError)// 2.51
    df.stat.crosstab("StockCode","Quantity").show()

    import org.apache.spark.sql.functions.monotonically_increasing_id
    df.select(monotonically_increasing_id()).show(2)




  }
}
