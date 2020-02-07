package com.zy.sparkvolumn.chapter02

import com.zy.sparkvolumn.SparkUtil

/**
  * create 2020-02-07
  * @author zhouyu
  * desc 从并行集合创建RDD
  */
object ParallelCollection {
  def main(args: Array[String]): Unit = {
    val session = SparkUtil.getSession
    //集合
    val rdd_one = session.sparkContext.parallelize(Seq(1,2,3))
    rdd_one.foreach(println)
  }
}
