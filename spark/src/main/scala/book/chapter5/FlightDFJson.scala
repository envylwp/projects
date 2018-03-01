package book.chapter5

import org.apache.spark.sql.{SparkSession}

object FlightDFJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val df = spark.read.format("json")
      .load("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/json/2015-summary.json")

    df.printSchema()
    df.schema.printTreeString()
    println(df.schema)

    import org.apache.spark.sql.types.{StructField,StructType,StringType,LongType}
    import org.apache.spark.sql.types.Metadata
//{"ORIGIN_COUNTRY_NAME":"South Korea","DEST_COUNTRY_NAME":"United States","count":827}
    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME",StringType,true),
      StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      StructField("count",LongType,false,Metadata.fromJson("{\"hello:\":\"2018-01-30 00:00:00\"}"))))


    val df2 = spark.read.format("json").schema(myManualSchema)
    .load("/mnt/disk/IdeaProjects/projects/spark/data/flight-data/json/2016-summary.json")
    df2.show(10)
    val flag  = (((df2.col("count")+5)*200)-6) < df2.col("count")
    import org.apache.spark.sql.functions.expr
    expr("(((someCol + 5) * 200) - 6) <otherCol")
    println(flag)

    df2.columns

    import org.apache.spark.sql.Row
    val myRow = Row("Hello",null,1,false)
    myRow(0)// type Any
    myRow(0).asInstanceOf[String]// String
    myRow.getString(0)// String
    myRow.getInt(2)// Int




  }

}
