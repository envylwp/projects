package book.chapter14

import org.apache.spark.sql.SparkSession

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)

    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
      "Big" -> -300, "Simple" -> 100)
    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)
    suppBroadcast.value

    val r = words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect()

    println(r.mkString("\n"))

  }
}
