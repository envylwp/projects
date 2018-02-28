package structstreaming


import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
/**
  * Created by lancerlin on 2018/1/7. 
  */
object StructuredNetworkWordCount {
  def main(args: Array[String]): Unit = {

    //LoggerLevels.setStreamingLogLevels

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
        .master("local")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.18.88")
      .option("port", 10001)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("update")
      .format("console")
      .start()

    println(query.toString)

    query.awaitTermination()
  }
}
