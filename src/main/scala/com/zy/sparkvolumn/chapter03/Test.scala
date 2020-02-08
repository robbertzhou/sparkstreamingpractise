package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil

/**
  * create 2020-02-07
  * author zhouyu
  * desc 测试全代码生成技术的性能差异：使用技术为8s，否则是1min
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
//    spark.conf.set("spark.sql.codegen.wholeStage",false)
    val cnt = spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(),"id").count()
    println("统计数为：" +cnt)
    Thread.sleep(2000000)
  }
}
