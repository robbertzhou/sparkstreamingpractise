package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil

/**
  * create 2020-02-07
  * author zhouyu
  * desc spark支持多种数据源创建dataframe,也支持多种数据格式：csv、json、parquet、orc等常用的格式
  */
object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    val df = spark.read.json("file:///I:\\testdata\\grade.json")
    df.show()
  }
}
