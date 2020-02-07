package com.zy.sparkvolumn.chapter02

import com.zy.sparkvolumn.SparkUtil

/**
  * create 2020-02-07
  * author zhouyu
  * desc 从hdfs获取数据集
  */
object FromHDFS {
  def main(args: Array[String]): Unit = {
    val session = SparkUtil.getSession
    val src = session.sparkContext.textFile("hdfs://master.zy.com:8020/data/myuser/part-m-00000")
    src.foreach(println)
  }
}
