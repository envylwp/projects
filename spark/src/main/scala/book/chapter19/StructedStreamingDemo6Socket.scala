package book.chapter19

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{ForeachWriter, SparkSession}

object StructedStreamingDemo6Socket {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val socketDF = spark.readStream.format("socket").option("host","localhost")
      .option("port","8888").load()

    val query = socketDF.writeStream.
      trigger(ProcessingTime.create(10, TimeUnit.SECONDS)).format("console").start()

    query.awaitTermination()
    // in memory
    //socketDF.writeStream.format("memory").queryName("")

    import org.apache.spark.sql.streaming.Trigger
    socketDF.writeStream.trigger(Trigger.ProcessingTime("100 seconds"))
      .format("console").outputMode("complete").start()

    import org.apache.spark.sql.streaming.Trigger
    socketDF.writeStream.trigger(Trigger.Once())
      .format("console").outputMode("complete").start()


  }
}
