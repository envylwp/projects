package com.projects

/**
  * Created by Lancerlin
  * date: 8/31/18 3:56 PM
  *
  */
object BlockExpressionDemo {
  def main(args: Array[String]) {
    val x = 0
    //在scala中{}中课包含一系列表达式，块中最后一个表达式的值就是块的值
    //下面就是一个块表达式
    val result = {
      if (x < 0){
        -1
      } else if(x >= 1) {
        1
      } else {
        "error"
      }
    }
    //result的值就是块表达式的结果
    println(result)
  }

}
