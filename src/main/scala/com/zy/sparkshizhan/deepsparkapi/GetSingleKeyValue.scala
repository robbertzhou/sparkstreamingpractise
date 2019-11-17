package com.zy.sparkshizhan.deepsparkapi

object GetSingleKeyValue {
  def main(args: Array[String]): Unit = {
    val transByCust = Common.getTransCust()
    transByCust.lookup(53).foreach(tran=>println(tran.mkString(",")))
  }
}
