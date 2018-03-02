package book.chapter6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DFDemo4 {
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

    println("------------------------------Working with Strings------------------------------------")
    import org.apache.spark.sql.functions.{initcap, col}
    df.select(initcap(col("Description"))).show(2,false)
    spark.sql("""SELECT initcap(Description) FROM dfTable""").show(2)

    import org.apache.spark.sql.functions.{lower,upper}
    df.select(col("Description"),
      lower(col("Description")),
      upper(lower(col("Description")))).show(2)
    spark.sql("""SELECT Description,lower(Description),Upper(lower(Description)) FROM dfTable""").show(2)

    import org.apache.spark.sql.functions.{lit,ltrim,rtrim,rpad,lpad,trim}
    df.select(
      ltrim(lit(" HELLO ")).as("ltrim"),
      rtrim(lit(" HELLO ")).as("rtrim"),
      trim(lit(" HELLO ")).as("trim"),
      lpad(lit("HELLO"),3," ").as("lp"),
      rpad(lit("HELLO"),10," ").as("rp")).show(2)
    spark.sql("""
              SELECT
              ltrim(' HELLLOOOO '),
              rtrim(' HELLLOOOO '),
              trim(' HELLLOOOO '),
              lpad('HELLOOOO ', 3, ' '),
              rpad('HELLOOOO ', 10, ' ')
              FROM dfTable
              """).show(2)

    println("------------------------------Working with Strings------------------------------------")
    import org.apache.spark.sql.functions.regexp_replace
    val simpleColors=Seq("black","white","red","green","blue")
    val regexString=simpleColors.map(_.toUpperCase).mkString("|")
    // the | signifies `OR` in regular expression syntax
    df.select(
      regexp_replace(col("Description"),regexString,"COLOR").alias("color_clean"),
      col("Description")).show(2)
    spark.sql("""SELECT
                regexp_replace(Description,'BLACK|WHITE|RED|GREEN|BLUE','COLOR')as
                color_clean,Description
                FROM dfTable""").show(2)


    import org.apache.spark.sql.functions.translate
    df.select(translate(col("Description"),"LEET","1337"),col("Description"))
      .show(2)
    spark.sql("""SELECT translate(Description,'LEET','1337'),Description FROM dfTable""").show(2)


    import org.apache.spark.sql.functions.regexp_extract
    val regexString2=simpleColors.map(_.toUpperCase).mkString("(","|",")")
    // the | signifies OR in regular expression syntax
    df.select(
      regexp_extract(col("Description"),regexString2,1).alias("color_clean"),
      col("Description")).show(2)
    spark.sql("""SELECT regexp_extract(Description,'(BLACK|WHITE|RED|GREEN|BLUE)',1),
                 Description
                 FROM dfTable""").show(2)

    val containsBlack=col("Description").contains("BLACK")
    val containsWhite=col("DESCRIPTION").contains("WHITE")
    df.withColumn("hasSimpleColor",containsBlack.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description").show(3,false)

    spark.sql("""SELECT Description FROM dfTable
                WHERE instr(Description,'BLACK')>=1 OR instr(Description,'WHITE')>=1""").show(2)



    println("------------------------------Working with Dates and Timestamps------------------------------------")
    df.printSchema()

    import org.apache.spark.sql.functions.{current_date,current_timestamp}
    val dateDF=spark.range(10)
      .withColumn("today",current_date())
      .withColumn("now",current_timestamp())
    dateDF.createOrReplaceTempView("dateTable")

    dateDF.printSchema()
    dateDF.show(5, false)

    import org.apache.spark.sql.functions.{date_add,date_sub}
    dateDF.select(date_sub(col("today"),5),date_add(col("today"),5)).show(1)
    spark.sql("""SELECT date_sub(today,5),date_add(today,5) FROM dateTable""").show(2)


    import org.apache.spark.sql.functions.{datediff,months_between,to_date}
    dateDF.withColumn("week_ago",date_sub(col("today"),7))
      .select(datediff(col("week_ago"),col("today"))).show(1)
    dateDF.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"),col("end"))).show(1)
    spark.sql("""SELECT to_date('2016-01-01'),months_between('2016-01-01','2017-01-01'),
                datediff('2016-01-01','2017-01-01')
                FROM dateTable""").show(2)


    import org.apache.spark.sql.functions.{to_date,lit}
    spark.range(5).withColumn("date",lit("2017-01-01"))
      .select(to_date(col("date"))).show(1)

    import org.apache.spark.sql.functions.to_date
    val dateFormat="yyyy-dd-MM"
    val cleanDateDF=spark.range(1).select(
      to_date(lit("2017-12-11"),dateFormat).alias("date"),
      to_date(lit("2017-20-12"),dateFormat).alias("date2"))
    cleanDateDF.createOrReplaceTempView("dateTable2")
    spark.sql("""SELECT to_date(date,'yyyy-dd-MM'),to_date(date2,'yyyy-dd-MM'),to_date(date)
                FROM dateTable2""").show(2)

    println("------------------------------Timestamps------------------------------------")

    import org.apache.spark.sql.functions.to_timestamp
    cleanDateDF.select(to_timestamp(col("date"),dateFormat)).show()
    spark.sql("""SELECT to_timestamp(date,'yyyy-dd-MM'),to_timestamp(date2,'yyyy-dd-MM')
                  FROM dateTable2""").show()

    spark.sql("""SELECT cast(to_date("2017-01-01","yyyy-dd-MM") as timestamp)""").show(2)



    println("------------------------------Working with Nulls in Data------------------------------------")
    import org.apache.spark.sql.functions.coalesce
    df.select(coalesce(col("Description"),col("CustomerId"))).show(false)


    println("------------------------------ifnull, nullIf, nvl, and nvl2------------------------------------")
    spark.sql("""SELECT
                ifnull(null,'return_value'),
                nullif('value','value'),
                nvl(null,'return_value'),
                nvl2('not_null','return_value',"else_value")
                FROM dfTable LIMIT 1""").show(2)


    df.na.drop().show(2)
    df.na.drop("any").show(2)
    df.na.drop("all")
    spark.sql("""SELECT * FROM dfTable WHERE Description IS NOT NULL""").show(2)

    df.na.drop("all",Seq("StockCode","InvoiceNo"))



    println("------------------------------fill------------------------------------")
    df.na.fill(5,Seq("StockCode","InvoiceNo"))
    val fillColValues=Map("StockCode"->5,"Description"->"No Value")
    df.na.fill(fillColValues)


    df.na.replace("Description",Map(""->"UNKNOWN"))

    println("------------------------------Working with Complex Types------------------------------------")

    df.selectExpr("(Description, InvoiceNo) as complex","*").show(2, false)
    import org.apache.spark.sql.functions.struct
    val complexDF=df.select(struct("Description","InvoiceNo").alias("complex"))
    complexDF.createOrReplaceTempView("complexDF")

    complexDF.select("complex.Description")
    complexDF.select(col("complex").getField("Description"))
    complexDF.select("complex.*")
    spark.sql("""SELECT complex.* FROM complexDF""").show(2)


    println("------------------------------Array------------------------------------")
    import org.apache.spark.sql.functions.split
    df.select(split(col("Description")," ")).show(2, false)
    spark.sql("""SELECT split(Description,' ') FROM dfTable""").show(2,false)

    df.select(split(col("Description")," ").alias("array_col"))
      .selectExpr("array_col[0]").show(2)
    spark.sql("""SELECT split(Description,' ')[0] FROM dfTable""").show(2,false)


    import org.apache.spark.sql.functions.size
    df.select(size(split(col("Description")," "))).show(2)// shows 5 and 3

    import org.apache.spark.sql.functions.array_contains
    df.select(array_contains(split(col("Description")," "),"WHITE")).show(2)
    spark.sql(""" SELECT array_contains(split(Description,' '),'WHITE') FROM dfTable""").show(2,false)


    println("------------------------------explode------------------------------------")
    import org.apache.spark.sql.functions.{split,explode}

    df.withColumn("splitted",split(col("Description")," "))
      .withColumn("exploded",explode(col("splitted")))
      .select("Description","InvoiceNo","exploded").show(2)

    spark.sql(""" SELECT Description,InvoiceNo,exploded
                  FROM(SELECT *,split(Description," ") as splitted FROM dfTable)
                  LATERAL VIEW explode(splitted) as exploded""").show(2,false)


    println("------------------------------Maps------------------------------------")
    import org.apache.spark.sql.functions.map
    df.select(map(col("Description"),col("InvoiceNo")).alias("complex_map")).show(2, false)
    spark.sql(""" SELECT map(Description,InvoiceNo) as complex_map FROM dfTable
                WHERE Description IS NOT NULL """).show(2,false)

    df.select(map(col("Description"),col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['WHITE METAL LANTERN']").show(5)

    df.select(map(col("Description"),col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)").show(5, false)

    println("------------------------------Working with JSON------------------------------------")
    val jsonDF=spark.range(1).selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

    import org.apache.spark.sql.functions.{get_json_object,json_tuple}
    jsonDF.select(
      get_json_object(col("jsonString"),"$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"),"myJSONKey")).show(2, false)
    jsonDF.selectExpr(
      "json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as column").show(2)

    import org.apache.spark.sql.functions.to_json
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct"))).show(2)

    import org.apache.spark.sql.functions.from_json
    val parseSchema = new StructType(Array(
                          new StructField("InvoiceNo", StringType, true),
                          new StructField("Description", StringType, true)))
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("newJSON"))
      .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2, false)






  }

}
