package com.projects.collection.array

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Lancerlin
  * date: 8/31/18 4:30 PM
  */
object ForArrayDemo {
  def main(args: Array[String]): Unit = {
    //初始化一个数组
    val arr = Array(1, 2, 3)
    //增强for循环
    for (i <- arr)
      println(i)

    //好用的until会生成一个Range
    //reverse是将前面生成的Range反转
    for (i <- (0 until arr.length).reverse)
      println(arr(i))
    println("----------------------")
    val ab = new ArrayBuffer[Int]()
    ab += 1
    //追加多个元素
    ab += (2, 3, 4, 5)

    for (i <- ab) {
      println(i)
    }

    for (i <- (0 until ab.length).reverse)
      println(ab(i))
  }
}
