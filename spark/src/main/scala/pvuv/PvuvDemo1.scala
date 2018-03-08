package pvuv

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

object PvuvDemo1 {

  case class PVUV(exec_date: String,
                  exec_hhmm: String,
                  bus_type: String,
                  tag: String,
                  sip: String,
                  uip: String,
                  domain: String
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
      val exec_date = ss(0).substring(0, 10)
      val exec_hhmm = ss(0).substring(11, 16)
      var tag = "null"
      if (StringUtils.isNotBlank(ss(2))) {
        tag = ss(2)
      }
      val sip = ss(4)
      val uip = ss(5)
      val domain = ss(6)
      val platform = ss(15)
      var bus_type = ""
      if (!domain.contains("hot")) {

        if (domain.contains("ttttt")) {

          bus_type = "ttttt"
        }
        if (domain.contains("fffff")) {
          bus_type = "fffff"
        }
      }
      PVUV(exec_date, exec_hhmm, bus_type, tag,
        sip, uip, domain)
    }).filter(pvuv => StringUtils.isNotBlank(pvuv.domain))
      .createOrReplaceTempView("pvuv")


    ds.show()

    spark.stop()


  }

}
