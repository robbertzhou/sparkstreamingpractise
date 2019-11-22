package com.zy.projectcase.realtimeprocessword

import com.zy.SendMsg

import scala.util.Random

object ProduceWord {
  def main(args: Array[String]): Unit = {
    val str = Array("上海","北京","广州","深圳")
    while (true){
      val rand = new Random()
      val msg = str(rand.nextInt(str.size))
      SendMsg.produceMsg("wordCount",msg)
      Thread.sleep(5)
    }

  }
}
