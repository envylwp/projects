package utils

import java.util

import com.google.gson.Gson

object GsonUtils {

  private val gson = new Gson()

  def getGson = {
    gson
  }

  def toJsonString(o: Any) = {
    gson.toJson(o)
  }

//  def fromJsonString[t:Object](str: String): Unit = {
//    gson.fromJson(str, classOf[t])
//  }



    def main(args: Array[String]): Unit = {
      val map = new util.HashMap[String, String]()
      map.put("a", "1")
      val event = new JsonEvnet(map, "hzy is my girlfriend")
      val str = GsonUtils.toJsonString(event)
      println(str)

      val jsonEvnet = GsonUtils.getGson.fromJson(str, classOf[JsonEvnet])
      println(jsonEvnet)
      println(jsonEvnet.headers)
      println(jsonEvnet.body)


  }
}
