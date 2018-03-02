package book.chapter5

import org.apache.spark.sql.SparkSession

object DFDemo2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val df = spark.read.format("json")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/json/2015-summary.json")

    df.createOrReplaceTempView("dataFrameTable")

    spark.sql("SELECT * FROM dataFrameTable").show(10)
    spark.sql("SELECT ORIGIN_COUNTRY_NAME,DEST_COUNTRY_NAME, count FROM dataFrameTable").show(10)
    //{"ORIGIN_COUNTRY_NAME":"India","DEST_COUNTRY_NAME":"United States","count":62}
    spark.sql("SELECT ORIGIN_COUNTRY_NAME,DEST_COUNTRY_NAME, count*10 FROM dataFrameTable").show(10)

    df.select("DEST_COUNTRY_NAME").show(2)
    df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)


    import org.apache.spark.sql.functions.{expr,col,column}
    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      expr("DEST_COUNTRY_NAME")
    ).show(2)



    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

    df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)

    df.selectExpr("DEST_COUNTRY_NAME as newColumnName","DEST_COUNTRY_NAME").show(2)

    df.selectExpr(
      "*",// include all original columns
      "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
      .show(2)

    spark.sql(
      """
        SELECT *,(DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME) as withinCountry
        FROM dataFrameTable
        LIMIT 2
      """.stripMargin).show(10)



    //avg
    spark.sql(
      """
        SELECT avg(count),count(distinct(DEST_COUNTRY_NAME))
        FROM dataFrameTable
        LIMIT 2
      """.stripMargin).show(10)
    df.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show(2)


    //Converting to Spark Types (Literals)
    import org.apache.spark.sql.functions.lit
    df.select(expr("*"),lit(1).as("One")).show(2)
    spark.sql("SELECT *,1 as One FROM dataFrameTable LIMIT 2").show()

    //Adding Columns
    df.withColumn("numberOne",lit(1)).show(2)
    spark.sql("SELECT *,1 as numberOne FROM dataFrameTable LIMIT 2").show()

    df.withColumn("withinCountry",expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
      .show(2)
    println(df.withColumn("Destination",expr("DEST_COUNTRY_NAME")).columns.mkString(","))


    //Renaming Columns
    println(df.withColumnRenamed("DEST_COUNTRY_NAME","dest").columns.mkString(","))


    //关键字 使用  `
    import org.apache.spark.sql.functions.expr

    val dfWithLongColName=df.withColumn(
      "This Long Column-Name",
      expr("ORIGIN_COUNTRY_NAME"))
    println(dfWithLongColName.columns.mkString(","))

    dfWithLongColName.selectExpr(
      "`This Long Column-Name`",
      "`This Long Column-Name` as `new col`")
      .show(2)

    dfWithLongColName.createOrReplaceTempView("dfTableLong")
    spark.sql(
      """
        SELECT `This Long Column-Name`,`This Long Column-Name` as `newcol`
        FROM dfTableLong LIMIT 2
      """.stripMargin).show(2)

    dfWithLongColName.select(col("This Long Column-Name")).columns


    //Spark默认不区分大小写; 不过，您可以通过设置配置使Spark区分大小写：  setspark.sql.caseSensitive  true


    //removing columns
    df.drop("ORIGIN_COUNTRY_NAME").columns
    dfWithLongColName.drop("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").show(2)

    //Changing a Column’s Type (cast)
    df.withColumn("count2",col("count").cast("long")).show(2)
    spark.sql(
      """
        SELECT *, cast(count as long) AS count2 FROM dataFrameTable
      """.stripMargin).show(2)


    println("------------------------------Filtering Rows------------------------------------")
    //Filtering Rows
    df.filter(col("count")<2).show(2)
    df.where("count <2").show(2)
    spark.sql(
      """
        SELECT *
        FROM
        dataFrameTable
        WHERE count<2 LIMIT 2
      """.stripMargin).show(2)

    df.where(col("count")<2).where(col("ORIGIN_COUNTRY_NAME")=!="Croatia")
      .show(2)
    df.where(col("count")<2).where(col("ORIGIN_COUNTRY_NAME")!=="Croatia")
      .show(2)
    spark.sql(
      """
        SELECT *
        FROM
        dataFrameTable
        WHERE count<2 AND ORIGIN_COUNTRY_NAME != "Croatia" LIMIT 2
      """.stripMargin).show(2)

    println("------------------------------Getting Unique Rows------------------------------------")
    df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct().count()
    spark.sql(
      """
        SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME,DEST_COUNTRY_NAME))
        FROM
        dataFrameTable
      """.stripMargin).show(2)

    df.select("ORIGIN_COUNTRY_NAME").distinct().count()
    spark.sql(
      """
        SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME)
        FROM
        dataFrameTable
      """.stripMargin).show(2)


    println("------------------------------Random Samples------------------------------------")

    val seed = 5
    val withReplacement=false
    val fraction=0.5
    println(df.sample(withReplacement,fraction,seed).count())

    println("------------------------------Random Splits------------------------------------")
    val dataFrames=df.randomSplit(Array(0.25,0.75),seed)
    dataFrames(0).count()>dataFrames(1).count()// False

    println("------------------------------Union------------------------------------")
    import org.apache.spark.sql.Row
    val schema=df.schema
    val newRows=Seq(
      Row("New Country","Other Country",5L),
      Row("New Country 2","Other Country 3",1L)
    )
    val parallelizedRows=spark.sparkContext.parallelize(newRows)
    val newDF=spark.createDataFrame(parallelizedRows,schema)
    df.union(newDF)
      .where("count = 1")
      .where(col("ORIGIN_COUNTRY_NAME")=!="United States")
      .where(col("DEST_COUNTRY_NAME")=!="United States")
      .show()// get all of them and we'll see our new rows at the end

    println("------------------------------Sorting Rows------------------------------------")

    df.sort("count").show(5)
    df.orderBy("count","DEST_COUNTRY_NAME").show(5)
    df.orderBy(col("count"),col("DEST_COUNTRY_NAME")).show(5)

    import org.apache.spark.sql.functions.{desc,asc}
    df.orderBy(expr("count desc")).show(2)
    df.orderBy(desc("count"),asc("DEST_COUNTRY_NAME")).show(2)
    spark.sql(
      """
        SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME)
        FROM
        dataFrameTable
        ORDER BY count DESC,DEST_COUNTRY_NAME ASC LIMIT 2
      """.stripMargin).show(2)

    spark.read.format("json").load("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/json/*-summary.json")
      .sortWithinPartitions("count")



    println("------------------------------Sorting Rows------------------------------------")
    df.limit(5).show()
    spark.sql(
      """
        SELECT *
        FROM dataFrameTable
        LIMIT 6
      """.stripMargin).show(2)

    df.orderBy(expr("count desc")).limit(6).show()

    spark.sql(
      """
        SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME)
        FROM dataFrameTable
        ORDER BY count DESC LIMIT 6
      """.stripMargin).show(2)


    println("------------------------------Repartition and Coalesce------------------------------------")
    df.rdd.getNumPartitions// 1
    df.repartition(5)
    df.repartition(col("DEST_COUNTRY_NAME"))  //repartition by col
//    You can optionally specify the number of partitions you would like, too:
      df.repartition(5,col("DEST_COUNTRY_NAME"))
    df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)

    println("------------------------------Collecting Rows to the Driver------------------------------------")

    val collectDF=df.limit(10)
    collectDF.take(5)// take works with an Integer count
    collectDF.show()// this prints it out nicely
    collectDF.show(5,false)
    collectDF.collect()
    collectDF.toLocalIterator()


  }















}
