package com.projects.collection.array.collection.map.set

import scala.collection.immutable.HashSet

/**
  * Created by Lancerlin
  * date: 8/31/18 5:14 PM
  */
object ImmutSetDemo {
  def main(args: Array[String]): Unit = {
    val set1 = new HashSet[Int]()
    //将元素和set1合并生成一个新的set，原有set不变
    val set2 = set1 + 4
    //set中元素不能重复
    val set3 = set1 ++ Set(5, 6, 7)
    val set0 = Set(1,3,4) ++ set1
    println(set0.getClass)

  }
}
