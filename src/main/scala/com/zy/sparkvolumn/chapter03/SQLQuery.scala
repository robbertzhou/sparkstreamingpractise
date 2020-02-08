package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil

/**
  * create 2020-02-08
  * author zhouyu
  * desc sql查询风格
  */
object SQLQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    val df = spark.read.json("file:///I:\\testdata\\grade.json")
//    df.groupBy("name","subject").avg("grade").show()
    df.groupBy("name").pivot("subject").avg("grade").show()
  }
}
