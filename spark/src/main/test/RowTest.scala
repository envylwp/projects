import org.apache.spark.sql.Row

object RowTest {
  def main(args: Array[String]): Unit = {
    val list = List("a", "b")

    val row = Row.fromSeq(list)
    println(row)

    val row1 = Row(list(0), list(1))
    println(row1)
  }

}
