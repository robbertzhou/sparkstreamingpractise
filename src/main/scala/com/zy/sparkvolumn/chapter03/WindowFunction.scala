package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import scala.reflect.api.materializeTypeTag
/**
  * create 2020-02-08
  * author zhouyu
  * desc 窗口函数
  */
object WindowFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    import spark.implicits._
    val df = spark.read.json("file:///I:\\testdata\\grade.json")
    val window = Window.partitionBy("name","subject").orderBy("grade")
    df.select(col("name"),col("subject")
      ,col("year"),col("grade"),row_number().over(window)).show()

  }
}
