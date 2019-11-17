package com.zy.sparkshizhan.deepsparkapi

import org.apache.spark.sql.SparkSession

object SparkJoinTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("baseRdd")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
  }
}
