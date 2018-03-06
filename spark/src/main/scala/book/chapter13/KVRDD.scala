package book.chapter13

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object KVRDD {
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

    println("------------------------------Key-Value Basics (Key-Value RDDs)------------------------------------")
    words.map(word => (word.toLowerCase, 1))

    println("------------------------------keyBy------------------------------------")
    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
    println(keyword.collect().mkString("  "))

    println("------------------------------Mapping over Values------------------------------------")
    val tuples = keyword.mapValues(word => word.toUpperCase).collect()
    println(tuples.mkString("  "))

    val tuples2 = keyword.flatMapValues(word => word.toUpperCase).collect()
    println(tuples2.mkString("  "))

    println("------------------------------Extracting Keys and Values------------------------------------")
    keyword.keys.collect()
    keyword.values.collect()


    println("------------------------------lookup------------------------------------")
    val strings = keyword.lookup("s")
    println(strings)

    println("------------------------------sampleByKey------------------------------------")
    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
      .collect()
    import scala.util.Random
    val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKey(true, sampleMap, 6L)
      .collect()

    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKeyExact(true, sampleMap, 6L).collect()

    println("------------------------------Aggregations------------------------------------")
    val chars = words.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))

    def maxFunc(left: Int, right: Int) = math.max(left, right)

    def addFunc(left: Int, right: Int) = left + right

    val nums = spark.sparkContext.parallelize(1 to 30, 5)


    println("------------------------------countByKey------------------------------------")
    val timeout = 1000L
    //milliseconds
    val confidence = 0.95
    KVcharacters.countByKey()
    KVcharacters.countByKeyApprox(timeout, confidence)


    println("------------------------------groupByKey------------------------------------")
    KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()

    println("------------------------------reduceByKey------------------------------------")
    KVcharacters.reduceByKey(addFunc).collect()

    println("------------------------------aggregate------------------------------------")
    nums.aggregate(0)(maxFunc, addFunc)


    println("------------------------------aggregate------------------------------------")
    val depth = 3
    nums.treeAggregate(0)(maxFunc, addFunc, depth)


    println("------------------------------aggregateByKey------------------------------------")
    KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()

    println("------------------------------combineByKey------------------------------------")
    val valToCombiner = (value: Int) => List(value)
    val mergeValuesFunc = (vals: List[Int], valToAppend: Int) => valToAppend :: vals
    val mergeCombinerFunc = (vals1: List[Int], vals2: List[Int]) => vals1 ::: vals2
    // now we define these as function variables
    val outputPartitions = 6
    KVcharacters
      .combineByKey(
        valToCombiner,
        mergeValuesFunc,
        mergeCombinerFunc,
        outputPartitions)
      .collect()


    println("------------------------------foldByKey------------------------------------")
    KVcharacters.foldByKey(0)(addFunc).collect()

    println("------------------------------CoGroups------------------------------------")
    import scala.util.Random
    val distinctChars2 = words.flatMap(word => word.toLowerCase.toSeq).distinct
    val charRDD = distinctChars2.map(c => (c, new Random().nextDouble()))
    val charRDD2 = distinctChars2.map(c => (c, new Random().nextDouble()))
    val charRDD3 = distinctChars2.map(c => (c, new Random().nextDouble()))
    charRDD.cogroup(charRDD2, charRDD3).take(5)

    println("------------------------------Join------------------------------------")
    val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
    val outputPartitions2 = 10
    //    KVcharacters.join(keyedChars).count()
    //    KVcharacters.join(keyedChars,outputPartitions2).count()
    //    • fullOuterJoin
    //
    //    • leftOuterJoin
    //
    //    • rightOuterJoin
    //
    //    • cartesian (This, again, is very dangerous! It does not accept a join key and can have a massive output.)

    println("------------------------------zips------------------------------------")
    val numRange = spark.sparkContext.parallelize(0 to 9, 2)
    words.zip(numRange).collect()


    println("------------------------------coalesce------------------------------------")
    words.coalesce(1).getNumPartitions // 1

    println("------------------------------repartition------------------------------------")
    words.repartition(10) // gives us 10 partitions

    println("------------------------------Custom Partitioning------------------------------------")
    val df = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("/data/retail-data/all/")
    val rdd = df.coalesce(10).rdd

    df.printSchema()

    import org.apache.spark.HashPartitioner
    rdd.map(r => r(6)).take(5).foreach(println)
    val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
    keyedRDD.partitionBy(new HashPartitioner(10)).take(10)


    keyedRDD
      .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
      .take(5)


    println("------------------------------Custom Serialization------------------------------------")
//    val conf=new SparkConf()
//    conf.registerKryoClasses(Array(classOf[MyClass1],classOf[MyClass2]))
//    val sc = new SparkContext(conf)

  }

}

// in Scala
import org.apache.spark.Partitioner

class DomainPartitioner extends Partitioner {
  def numPartitions = 3

  def getPartition(key: Any): Int = {
    val customerId = key.asInstanceOf[Double].toInt
    if (customerId == 17850.0 || customerId == 12583.0) {
      return 0
    } else {
      return new java.util.Random().nextInt(2) + 1
    }
  }
}
