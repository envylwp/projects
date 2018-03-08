package pvuv

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

object PvuvDemo3Window {

  case class PVUV2(time: String,
                   tag: String,
                   uip: String,
                   domain: String
                  )

  case class PVUVEvent(exec_hhmm: String,
                   exec_date: String,
                   bus_type: String,
                   domain: String,
                   pv: Long,
                   uv: Long,
                   etl_time: String
                  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()


    import spark.implicits._
    val ds = spark.read.textFile("/mnt/disk/IdeaProjects/projects/spark/data/pvuv/pvuv-1.txt").as[String]
    //2016-05-18 21:00:00||90FA453C1F627BA8271280548BF05D66|9597331-AD37DD5|175.171.190.193|192.168.1.22|m.mall.fffff.com|/app/merch/index.html||||||414x736|zh-cn|iPhone||1|WebKit|601.1.46||||iOS 9.3.2|2CA8BB90-7057-4722-BAF2-C44AE6FB8846|h5_nested_ios
    val pvuvds = ds.map(line => {
      val ss = line.split("\\|")
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
        domain: String
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

    val all = spark.sql(
      """
        select  SUBSTR(time,12,5) as exec_hhmm, SUBSTR(time,1,10) as exec_date, tag,
        'ALL' as bus_type, 'ALL' as domain, count(1) pv
        from click_stream_log
        where ( domain like '%fffff%' or domain like '%ttttt%' ) and domain not like '%hot'
        group by  SUBSTR(time,12,5) ,SUBSTR(time,1,10) ,tag
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

    domain.union(all).union(busType).createOrReplaceTempView("t_click_stream_log")

    val result = spark.sql(
      """
        select exec_hhmm,exec_date,bus_type, domain, sum(pv) as pv, count(distinct tag) as uv, now() as etl_time
        from t_click_stream_log
        group by exec_hhmm, exec_date, bus_type, domain
      """.stripMargin)

    val event = result.as[PVUVEvent]
    event.show(10, false)
    event.foreach(event => {
      println(s"${event.exec_hhmm}, ${event.exec_date},${event.bus_type},${event.domain},${event.pv},${event.uv},${event.etl_time}")
    })





    spark.stop()


  }

}
