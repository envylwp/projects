package utils
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.{DateUtils, FastDateFormat}
import org.slf4j.{Logger, LoggerFactory}

object DateTimeUtilsV1 {
  @transient lazy val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  val DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val DAY_SECONDS = 86400

  private val formater: FastDateFormat = FastDateFormat.getInstance(DEFAULT_DATETIME_FORMAT)

  /**
    * 字符串转为日期
    * @param s
    * @return
    */
  def parse(s: String): Date = {
    try {
      formater.parse(s)
    } catch {
      case e: Exception => {
        LOG.error("parse dateStr error..", e)
        null
      }
    }
  }
  /**
    * 字符串转为timestamp
    * @param s
    * @return
    */
  def parseToTimeStamp(s:String ): Long = {
    parse(s).getTime
  }
}
