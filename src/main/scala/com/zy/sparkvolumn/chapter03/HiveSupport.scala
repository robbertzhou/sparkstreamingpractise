package com.zy.sparkvolumn.chapter03

import org.apache.spark.sql.SparkSession

/**
  * create 2020-02-09
  * author zhouyu
  * desc hivespark
  */
object HiveSupport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[2]")
      .enableHiveSupport()
      .getOrCreate
    spark.sql("select * from json_table").show()
  }
}
