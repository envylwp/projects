package structedstreaming

import org.apache.spark.sql.SparkSession

object NetWordCountOffical {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      //.config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    import spark.implicits._
    // 创建表示从连接到 localhost:9999 的输入行 stream 的 DataFrame
    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.18.88")
      .option("port", 9999)
      .load()

    // 将 lines 切分为 words
    val words = lines.as[String].flatMap(_.split(" "))

    // 生成正在运行的 word count
    val wordCounts = words.groupBy("value").count()

    // 开始运行将 running counts 打印到控制台的查询
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
