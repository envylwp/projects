package com.projects.collection.array

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Lancerlin
  * date: 8/31/18 4:03 PM
  */
object ArrayDemo {

  def main(args: Array[String]): Unit = {
    // 不可变数组
    val arr1 = new Array[Int](8)
    println(arr1)
    println(arr1.toBuffer)
    val buffer: mutable.Buffer[Int] = arr1.toBuffer

    val arr2 = Array[Int](10)
    println(arr2)
    println(arr2.toBuffer)


    val newarr2 = arr2.toBuffer += 1
    println("newarr2:" + newarr2)

    val arr3 = Array("hadoop", "storm", "spark")
    println(arr3(2))
    arr3(2) = "new spark"
    println(arr3(2))

    // 变长数组
    val ab = ArrayBuffer[Int]()
    //+=尾部追加元素
    ab += 1
    //追加多个元素
    ab += (2, 3, 4, 5)
    //追加一个数组++=
    ab ++= Array(6, 7)
    //追加一个数组缓冲
    ab ++= ArrayBuffer(8,9)
    //打印数组缓冲ab
    println(ab)
    ab.insert(0, -1, 0)
    println(ab)

    ab.remove(0)
    println(ab)


  }
}
