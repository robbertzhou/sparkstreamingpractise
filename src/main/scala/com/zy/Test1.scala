package com.zy

import org.apache.spark.sql.SparkSession

object Test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("first")
      .master("local[2]")
      .getOrCreate()

    val ghLogs = spark.read.json("file:///g:/test_data/sparkstreaming/2015-03-01-0.json")
    val pushes = ghLogs.filter("type='PushEvent'")
    pushes.printSchema()
    println("all events:" + ghLogs.count())
    print("only pushs:" + pushes.count())
  }
}
