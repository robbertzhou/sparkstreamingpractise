package com.zy.sparkshizhan.deepsparkapi

import java.util

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * 使用spark数据分区器
  */
object UseSparkDataPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
//      .config("spark.default.parallelism","30")
      .appName("dataPartition")
      .getOrCreate()
    //读取文件
    spark.sparkContext.getConf.set("spark.default.parallelism","4")
    val lines = spark.sparkContext.textFile("file:///g:/testdata/student.csv")
    lines.mapPartitionsWithIndex((partition,it)=>{
      val result = new ArrayBuffer[String]()
      it.foreach(fun=>{
        println("hello==" + partition)
      })
      result.iterator
    }).collect()
  }
}
