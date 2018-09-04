package com.projects.collection.array.collection.map

import scala.collection.mutable

/**
  * Created by Lancerlin
  * date: 8/31/18 4:50 PM
  */
object MapDemo {

  def main(args: Array[String]): Unit = {
    // 在Scala中，有两种Map，一个是immutable包下的Map，该Map中的内容不可变；另一个是mutable包下的Map，该Map中的内容可变
    val scores = Map("spark" -> 100, "hadoop" -> 80)
    println(scores)
    println(scores("spark"))
    println(scores.getOrElse("spark2", 0))

    val scores2 = Map(("spark2", 100), ("hadoop2", 80))
    println(scores2)


    println("============== mutable.Map ==============")
    val mmscores = mutable.Map("mmspark" -> 100, "mmhadoop" -> 80)
    mmscores.put("mmstorm", 90)
    mmscores.put("mmspark", 101)
    println(mmscores)


  }
}
