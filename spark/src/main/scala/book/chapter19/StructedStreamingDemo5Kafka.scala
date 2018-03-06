package book.chapter19

import org.apache.spark.sql.{ForeachWriter, SparkSession}

object StructedStreamingDemo5Kafka {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val ds1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    /**
      *

    // Subscribe to multiple topics
    val ds2=spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .option("subscribe","topic1,topic2")
      .load()

    // Subscribe to a pattern of topics
    val ds3=spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .option("subscribePattern","topic.*")
      .load()
      */
    ds1.selectExpr("topic","CAST(key AS STRING)","CAST(value AS STRING)")
      .writeStream.format("kafka")
      .option("checkpointLocation","/to/HDFS-compatible/dir")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .start()


    ds1.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .option("checkpointLocation","/to/HDFS-compatible/dir")
      .option("topic","topic1")
      .start()

    val writer = new ForeachWriter[String]{
      def open(partitionId:Long,version:Long):Boolean={
        // open a database connection
      }
      def process(record:String)={
        // write string to connection
      }
      def close(errorOrNull:Throwable):Unit={
        // close the connection
      }
    }


  }
}
