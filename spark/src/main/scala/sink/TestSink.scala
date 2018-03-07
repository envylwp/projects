package sink

import org.apache.spark.sql.{ForeachWriter, Row}

class TestSink extends ForeachWriter[Row]{
  override def open(partitionId: Long, version: Long): Boolean = {
    println(s"partitionId  ==== $partitionId, version ==== $version")

    true
  }

  override def process(row: Row): Unit = {
    println("====  process  ====")

  }
  override def close(errorOrNull: Throwable): Unit = {
    println(s"$errorOrNull")
  }
}
