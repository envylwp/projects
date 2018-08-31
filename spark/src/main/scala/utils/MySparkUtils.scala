package utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MySparkUtils extends Logging {

  logInfo("initializing MySparkUtils")

  var spark: SparkSession = _

  def init() = {
    if(spark == null){
      spark = SparkSession
        .builder
        .appName("kafkaConsumer")
        .master("local")
        .config("spark.driver.bindAddress", "20000")
        .getOrCreate()
    }
  }
}
