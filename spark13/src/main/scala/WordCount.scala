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

    sc.textFile(args(0)) //.cache()
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(args(1))
    sc.stop()

  }

}
