package com.zy.bigdataanalysis

import scala.util.Random

/**
  * create 2020-03-14
  * author zhouyu
  * desc 模式匹配
  */
object PatternMatch {
  def main(args: Array[String]): Unit = {
    val x = Random.nextInt(10)
    x match{
      case 0 => println("0000")
      case 1 => println("111")
      case 2 => println("2222")
      case 3 => println("333")
      case 4 => println("444")
      case 5 => println("555")
      case 6 => println("666")
      case 7 => println("777")
      case _ => println("other")
    }
  }
}
