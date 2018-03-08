import java.util

import org.apache.flume.Event

object GsonTest {
  def main(args: Array[String]): Unit = {
    new Event {override def setHeaders(map: util.Map[String, String]): Unit = ???

      override def getHeaders: util.Map[String, String] = ???

      override def getBody: Array[Byte] = ???

      override def setBody(bytes: Array[Byte]): Unit = ???
    }
  }
}
