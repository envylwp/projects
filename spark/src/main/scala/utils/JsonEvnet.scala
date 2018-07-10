package utils

import java.util

import com.google.gson.Gson


class JsonEvnet(_headers: util.Map[String, String], _body: String) extends Serializable {

  val headers: util.Map[String, String] = _headers
  val body = _body

  override def toString = s"JsonEvnet($headers, $body)"
}

object JsonEvnet {

}