import java.util

import scala.collection.{JavaConverters, mutable}

object CollectionTest {
  def main(args: Array[String]): Unit = {

//    testScalaList2JavaList

    val listJ = new util.ArrayList[Int]()
    listJ.add(1)
    listJ.add(2)
    listJ.add(3)
    listJ.add(4)
    val listS: mutable.Buffer[Int] = JavaConverters.asScalaBufferConverter(listJ).asScala
    println(listS)
    println(listJ)

  }

  private def testScalaList2JavaList = {
    val listS = List(1, 2, 3, 4)
    val listJ: util.Collection[Int] = JavaConverters.asJavaCollectionConverter(listS).asJavaCollection
    println(listS)
    println(listJ)
  }
}
