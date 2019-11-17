package com.zy.sparkshizhan.deepsparkapi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Common {
  def getTransCust(): RDD[(Int,Array[String])]  ={
    val spark = SparkSession.builder()
      .appName("baseRdd")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val tranFile = sc.textFile("file:///g:/test_data/spark/ch04_data_transactions.txt")
    val tranData = tranFile.map(_.split("#"))
    val transByCust = tranData.map(tran=>(tran(2).toInt,tran))
    return transByCust
  }
}
