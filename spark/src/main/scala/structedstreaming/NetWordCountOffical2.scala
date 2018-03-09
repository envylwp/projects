package structedstreaming

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

object NetWordCountOffical2 {
  case class DeviceData(device: String, deviceType: String, signal: Double, time: DateTime)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    // 创建表示从连接到 localhost:9999 的输入行 stream 的 DataFrame
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    socketDF.isStreaming    // 对于有 streaming sources 的 DataFrame 返回 True

    socketDF.printSchema

    // 读取目录内原子写入的所有 csv 文件
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema)      // 指定 csv 文件的模式
      .csv("/path/to/directory")    // 等同于 format("csv").load("/path/to/directory")

    val ds: Dataset[DeviceData] = csvDF.as[DeviceData]    // streaming Dataset with IOT device data

    // Select the devices which have signal more than 10
    csvDF.select("device").where("signal > 10")      // using untyped APIs
    ds.filter(_.signal > 10).map(_.device)         // using typed APIs

    // Running count of the number of updates for each device type
    csvDF.groupBy("deviceType").count()                          // using untyped API

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))    // using typed API
  }

}
