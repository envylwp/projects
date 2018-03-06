package book.chapter12

import org.apache.spark.sql.SparkSession

object RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    spark.range(500).rdd
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
    import spark.implicits._
    spark.range(10).rdd.toDF()

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)

    words.setName("myWords")
    words.name // myWords

    println("------------------------------From Data Sources------------------------------------")
    spark.sparkContext.textFile("/some/path/withTextFiles")
    spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")

    println("------------------------------distinct------------------------------------")
    words.distinct().count()

    println("------------------------------filter------------------------------------")

    def startsWithS(individual: String) = {
      individual.startsWith("S")
    }

    words.filter(word => startsWithS(word)).collect()

    println("------------------------------map------------------------------------")
    val words2 = words.map(word => (word, word(0), word.startsWith("S")))
    words2.filter(record => record._3).take(5)

    println("------------------------------flatMap------------------------------------")
    words.flatMap(word => word.toSeq).take(5)

    println("------------------------------sort------------------------------------")
    words.sortBy(word => word.length() * -1).take(2)

    println("------------------------------Random Splits------------------------------------")
    val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

    println("------------------------------Action------------------------------------")
    println("------------------------------reduce------------------------------------")
    spark.sparkContext.parallelize(1 to 20).reduce(_ + _)

    // 210
    def wordLengthReducer(leftWord: String, rightWord: String): String = {
      if (leftWord.length > rightWord.length)
        return leftWord
      else
        return rightWord
    }

    words.reduce(wordLengthReducer)

    println("------------------------------count------------------------------------")
    words.count()


    println("------------------------------countApprox------------------------------------")
    val confidence = 0.95
    val timeoutMilliseconds = 400
    words.countApprox(timeoutMilliseconds, confidence)

    println("------------------------------countApproxDistinct------------------------------------")
    words.countApproxDistinct(0.05)
    words.countApproxDistinct(4, 10)


    println("------------------------------countByValue------------------------------------")
    words.countByValue()

    println("------------------------------countByValueApprox------------------------------------")
    words.countByValueApprox(1000, 0.95)

    println("------------------------------first------------------------------------")
    words.first()

    println("------------------------------max and min------------------------------------")
    spark.sparkContext.parallelize(1 to 20).max()
    spark.sparkContext.parallelize(1 to 20).min()

    println("------------------------------take------------------------------------")
    words.take(5)
    words.takeOrdered(5)
    words.top(5)
    val withReplacement = true
    val numberToTake = 6
    val randomSeed = 100L
    words.takeSample(withReplacement, numberToTake, randomSeed)

    println("------------------------------Saving Files------------------------------------")

    println("------------------------------saveAsTextFile------------------------------------")
    words.saveAsTextFile("file:/tmp/bookTitle")
    import org.apache.hadoop.io.compress.BZip2Codec
    words.saveAsTextFile("file:/tmp/bookTitleCompressed", classOf[BZip2Codec])

    println("------------------------------SequenceFiles------------------------------------")
    words.saveAsObjectFile("/tmp/my/sequenceFilePath")

    println("------------------------------Caching------------------------------------")
    words.cache()
    words.getStorageLevel

    println("------------------------------Checkpointing------------------------------------")
    spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
    words.checkpoint()

    println("------------------------------Pipe RDDs to System Commands------------------------------------")
    words.pipe("wc -l").collect()

    println("------------------------------mapPartitions------------------------------------")
    words.mapPartitions(part => Iterator[Int](1)).sum() // 2

    def indexedFunc(partitionIndex: Int, withinPartIterator: Iterator[String]) = {
      withinPartIterator.toList.map(
        value => s"Partition: $partitionIndex=>$value").iterator
    }

    words.mapPartitionsWithIndex(indexedFunc).collect()

    println("------------------------------foreachPartition------------------------------------")
    words.foreachPartition { iter =>
      import java.io._
      import scala.util.Random
      val randomFileName = new Random().nextInt()
      val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
      while (iter.hasNext) {
        pw.write(iter.next())
      }
      pw.close()
    }

    println("------------------------------foreachPartition------------------------------------")
    spark.sparkContext.parallelize(Seq("Hello","World"),2).glom().collect()
    // Array(Array(Hello), Array(World))









  }

}
