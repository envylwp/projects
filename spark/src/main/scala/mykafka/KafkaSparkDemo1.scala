package mykafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import utils.{DateTimeUtilsV1, GsonUtils, JsonEvnet, MyKafkaUtils}

import scala.collection.JavaConverters

object KafkaSparkDemo1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("kafkaConsumer")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    val list = MyKafkaUtils.consumer()
    println(list.get(0))
    println(list.get(0).value())
    val records = JavaConverters.asScalaBufferConverter(list).asScala
    val rdd: RDD[JsonEvnet] = spark.sparkContext.parallelize({

      records.map(record => GsonUtils.getGson.fromJson(record.value(), classOf[JsonEvnet]))
    })

    val schemaString = "time,uid"

    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = rdd
      .map(_.body.split("\\|"))
      .map(attributes => {
        val fieldValues = new Array[Any](schema.size)
        val copyLen = if (attributes.length > schema.size) schema.size else attributes.length
        System.arraycopy(attributes, 0, fieldValues, 0, copyLen)
        fieldValues.foreach(println("::::", _))
        Row.fromSeq(fieldValues)
      })

    val valueDF = spark.createDataFrame(rowRDD, schema)
    valueDF.createOrReplaceTempView("root_tmp_view")
    val buffer = spark.sql("select * from root_tmp_view").collect().toBuffer
    println(buffer)

  }

}
