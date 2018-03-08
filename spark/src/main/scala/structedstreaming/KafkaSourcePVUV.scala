package structedstreaming

import com.google.gson.Gson
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.flume.event.{JSONEvent, SimpleEvent}
import org.apache.spark.sql.SparkSession
import utils.DateTimeUtils


case class PVUV2(time: String,
                 tag: String,
                 uip: String,
                 domain: String,
                 timestamp: Long
                )



case class PVUVMid(exec_hhmm: String,
                     exec_date: String,
                     bus_type: String,
                     domain: String,
                     pv: Long
                    )

case class PVUVEvent(exec_hhmm: String,
                     exec_date: String,
                     bus_type: String,
                     domain: String,
                     pv: Long,
                     uv: Long,
                     etl_time: String
                    )

object KafkaSourcePVUV {

  case class MyEvent(body: String, headers: util.Map[String, String])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    import spark.implicits._
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.1.50.122:9092,10.1.50.123:9092,10.1.50.124:9092")
      .option("subscribe", "lancer_test_clickstream_topic")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    lines.map( line =>{

      val gson = new Gson()
      val event = gson.fromJson(line, classOf[JSONEvent])
      val body = new String(event.getBody)
      val ss = body.split("\\|")
      val exec_datetime = ss(0)
      var tag = "null"
      if (StringUtils.isNotBlank(ss(2))) {
        tag = ss(2)
      }
      val uip = ss(5)
      val domain = ss(6)
      PVUV2(exec_datetime: String,
        tag: String,
        uip: String,
        domain: String,
        DateTimeUtils.parseToTimeStamp(exec_datetime)
      )
    }).createOrReplaceTempView("click_stream_log")


    val domain = spark.sql(
      """
          select
          SUBSTR(time,12,5) as  exec_hhmm, SUBSTR(time,1,10) as exec_date, tag,
          case when domain like '%ttttt%' then 'ttttt' else 'fffff' end as bus_type, domain, count(1) pv
          from click_stream_log
          where ( domain like '%fffff%' or domain like '%ttttt%' ) and domain not like '%hot'
          group by SUBSTR(time,12,5) ,SUBSTR(time,1,10),tag, case when domain like '%ttttt%' then 'ttttt' else 'fffff' end ,domain
      """.stripMargin)


    val busType = spark.sql(
      """
        select SUBSTR(time,12,5) as exec_hhmm, SUBSTR(time,1,10) as exec_date, tag,
        case when domain like '%ttttt%' then 'ttttt' else 'fffff' end as bus_type,
        'ALL' domain, count(1) pv
        from click_stream_log
        where( domain like '%fffff%' or domain like '%ttttt%' ) and domain not like '%hot'
        group by SUBSTR(time,12,5) ,SUBSTR(time,1,10) ,tag, case when domain like '%ttttt%' then 'ttttt' else 'fffff' end
      """.stripMargin)

    val all = spark.sql(
      """
        select  SUBSTR(time,12,5) as exec_hhmm, SUBSTR(time,1,10) as exec_date, tag,
        'ALL' as bus_type, 'ALL' as domain, count(1) pv
        from click_stream_log
        where ( domain like '%fffff%' or domain like '%ttttt%' ) and domain not like '%hot'
        group by  SUBSTR(time,12,5) ,SUBSTR(time,1,10) ,tag
      """.stripMargin).as[PVUVMid].createOrReplaceTempView("click_stream_log_mid")


   val mid =  spark.sql(
      """
             select exec_hhmm,exec_date,bus_type, domain, sum(pv) as pv, count(distinct tag) as uv, now() as etl_time
        from click_stream_log_mid
        group by exec_hhmm, exec_date, bus_type, domain
      """.stripMargin)


    val query = mid.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()


  }
}
