package book.chapter21

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object StructedStreamingDemo11Monitor {

  case class InputRow(user: String, timestamp: java.sql.Timestamp, activity: String)

  case class UserState(user: String,
                       var activity: String,
                       var start: java.sql.Timestamp,
                       var end: java.sql.Timestamp)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 5)
    val static = spark.read.json("/mnt/disk/IdeaProjects/projects/spark/data/activity-data")

    val streaming = spark
      .readStream
      .schema(static.schema)
      .option("maxFilesPerTrigger", 10)
      .json("/mnt/disk/IdeaProjects/projects/spark/data/activity-data")


    val query = streaming.writeStream.queryName("").format("").start()

    query.status


    query.recentProgress

    spark.streams.addListener(new StreamingQueryListener(){
      override def onQueryStarted(queryStarted:QueryStartedEvent):Unit={
        println("Query started: "+queryStarted.id)
      }
      override def onQueryTerminated(
        queryTerminated:QueryTerminatedEvent):Unit={
        println("Query terminated: "+queryTerminated.id)
      }
      override def onQueryProgress(queryProgress:QueryProgressEvent):Unit={
        println("Query made progress: "+queryProgress.progress)
      }
    })


  }
}
