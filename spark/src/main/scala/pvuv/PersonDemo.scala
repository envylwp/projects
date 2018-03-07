package pvuv

import org.apache.spark.sql.{Encoders, SparkSession}


object PersonDemo {

  case class Person(id: Integer, name: String, age: Integer)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.driver.bindAddress", "20000")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.read.textFile("/mnt/disk/IdeaProjects/projects/spark/data/person/1.txt").as[String]
    val view = ds.map(line => {
      val ss = line.split("\\|")
      Person(Integer.parseInt(ss(0)), ss(1), Integer.parseInt(ss(2)))
    })
    view.createOrReplaceTempView("person")
    spark.sql("SELECT * FROM person").show()
    spark.sql("SELECT name,count(1) FROM person where name='zhangsan' group by name").show()


    spark.stop()


  }

}
