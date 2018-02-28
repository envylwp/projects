import org.apache.spark.sql.SparkSession
import sink.TestSink

import scala.util.Random

/**
  * Created by lancerlin on 2018/1/7. 
  */
object NetWorkPerson {

  case class Person(name: String, age: Integer)


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
      .option("host", "10.1.50.219")
      .option("port", 10007)
      .load()

    // Split the lines into words
    val persons = lines.as[String].flatMap(_.split("\\s+")).map(Person(_, Random.nextInt(30))).toDF()
    val frame = persons.select("name", "age", "name")



    persons.createOrReplaceTempView("people")
//    val frame = spark.sql("select * from people")


//    val query = frame.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()

    val sink = new TestSink
    val query = frame.writeStream
          .outputMode("append")
            .foreach(sink)
          .start()



    println(query.toString)

    query.awaitTermination()
  }
}
