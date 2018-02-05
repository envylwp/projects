import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lancerlin on 2018/2/2. 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    //非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC")
    val sc = new SparkContext(conf)

    //textFile会产生两个RDD：HadoopRDD  -> MapPartitinsRDD
    sc.textFile(args(0)) //.cache()
      // 产生一个RDD ：MapPartitinsRDD
      .flatMap(_.split(" "))
      //产生一个RDD MapPartitionsRDD
      .map((_, 1))
      //产生一个RDD ShuffledRDD
      .reduceByKey(_ + _)
      //产生一个RDD: mapPartitions
      .saveAsTextFile(args(1))
    sc.stop()

  }

}
