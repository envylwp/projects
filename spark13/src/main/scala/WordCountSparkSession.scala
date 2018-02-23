import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lancerlin on 2018/2/23. 
  */
object WordCountSparkSession {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    val spark = SparkSession.builder
         .master("local")
         .appName("Word Count")
         .getOrCreate()


    val df = spark.read.textFile("spark13\\src\\main\\resources\\wc.txxt")
    df.show()

    spark.stop()



  }

}
