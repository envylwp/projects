
import org.apache.spark.sql.catalog.Column

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable

/**
  * Created by Lancerlin
  * date: 8/31/18 6:25 PM
  */
object ColumnTest {

  def main(args: Array[String]): Unit = {
    val abs = new ArrayBuffer[Column]()
    val c1 = new Column("fuid", "这是fuid", "int", false, false, false)
    val c2 = new Column("fagent", "这是fagent", "int", true, false, false)
    val c3 = new Column("fbind_time", "这是fbind_time", "timestamp", true, false, false)
    val c4 = new Column("fcreate_time", "这是fcreate_time", "timestamp", true, false, false)
    val c5 = new Column("fmodify_time", "这是fmodify_time", "timestamp", true, false, false)
    val c6 = new Column("fversion", "这是fversion", "int", true, false, false)
    val c7 = new Column("f_etl_db_name", "这是f_etl_db_name", "string", true, false, false)
    val c8 = new Column("f_etl_table_name", "这是f_etl_table_name", "string", true, false, false)
    val c9 = new Column("fetl_time", "这是fetl_time", "timestamp", true, false, false)
    val c10 = new Column("f_p_date", "这是f_p_date", "date", true, true, false)
    val c11 = new Column("fscene", "这是fscene", "int", true, true, false)
    //    println(c1)
//    abs += c1 += c2 += c3 += c4 += c5 += c6 += c7 += c8 += c9 += c10 += c11
    abs.foreach(println(_))

    val partitions: ArrayBuffer[String] = getPartitionsString(abs)
    println(partitions.isEmpty)

    val columns: ArrayBuffer[String] = buildSql(abs)
    println(columns.mkString(", "))
  }


  private def buildSql(abs: ArrayBuffer[Column]) = {
    val columns = new ArrayBuffer[String]()
    for (ab <- abs) {
      ab.name match {
        case "f_etl_db_name" => columns += s"headers.database_name AS ${ab.name}"
        case "f_etl_table_name" => columns += s"headers.table_name AS ${ab.name}"
        case "fetl_time" | "f_p_date" => columns += s"headers.exec_time AS ${ab.name}"
        case _ => {
          if (!ab.nullable) {
            ab.dataType match {
              case "string" => columns += s"nvl(${ab.name},'')  AS ${ab.name}"
              case "int" => columns += s"cast(nvl(${ab.name},'-1') AS INT) AS ${ab.name}"
              case "timestamp" => columns += s"nvl(${ab.name},'1970-01-01 00:00:00') AS ${ab.name}"
              case "date" => columns += s"nvl(${ab.name},'1970-01-01') AS ${ab.name}"
              case _ => columns += s"nvl(${ab.name},'') AS ${ab.name}"
            }
          } else {
            columns += s"body.${ab.name} AS ${ab.name}"
          }
        }
      }
    }
    columns
  }

  private def getPartitionsString(abs: ArrayBuffer[Column]) = {
    val partitions = new ArrayBuffer[String]()
    for (ab <- abs) {
      if (true.equals(ab.isPartition)) {
        println(ab.name)
        partitions += ab.name
      }
    }
    partitions
  }
}
