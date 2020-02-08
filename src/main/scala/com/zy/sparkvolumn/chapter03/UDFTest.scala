package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil
import org.apache.spark.sql.functions._

/**
  * create 2020-02-08
  * author zhouyu
  * desc 测试udf函数
  */
object UDFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    val df = spark.read.json("file:///I:\\testdata\\grade.json")
    val ut = new GradeUdf()
    val myUDF = udf(ut.level _)
    df.select(col("name"),myUDF(col("grade"))).show()
  }
}
