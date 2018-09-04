package com.projects.collection.array.collection.map.set

import scala.collection.mutable

/**
  * Created by Lancerlin
  * date: 8/31/18 5:24 PM
  */
object MutSetDemo {
  def main(args: Array[String]): Unit = {
    //创建一个可变的HashSet
    val set1 = new mutable.HashSet[Int]()
    //向HashSet中添加元素
    set1 += 2
    //add等价于+=
    set1.add(4)
    set1 ++= Set(1,3,5)
    println(set1)
    //删除一个元素
    set1 -= 5
    set1.remove(2)
    println(set1)

  }

}
