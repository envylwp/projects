package book.chapter5

import org.apache.spark.sql.SparkSession

object RowDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructField,StructType,StringType,LongType}
    val myManualSchema2 = new StructType(Array(
      new StructField("some",StringType,true),
      new StructField("col",StringType,true),
      new StructField("names",LongType,false)))
    val myRows = Seq(Row("Hello",null,1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD,myManualSchema2)
    myDf.show()

    import  spark.implicits._
    val myDF=Seq(("Hello",2,1L)).toDF("col1","col2","col3")
    myDF.show(1)
/**
    -- in SQL
    SELECT *FROM dataFrameTable
    SELECT columnName FROMdataFrameTable
    SELECT columnName*10,otherColumn,someOtherColasc FROM dataFrameTable

  df.select("DEST_COUNTRY_NAME").show(2)
  df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)


      importorg.apache.spark.sql.functions.{expr,col,column}
        df.select(
        df.col("DEST_COUNTRY_NAME"),
        col("DEST_COUNTRY_NAME"),
        column("DEST_COUNTRY_NAME"),
        'DEST_COUNTRY_NAME,
        $"DEST_COUNTRY_NAME",
        expr("DEST_COUNTRY_NAME"))
        .show(2)
 **/



  }

}
