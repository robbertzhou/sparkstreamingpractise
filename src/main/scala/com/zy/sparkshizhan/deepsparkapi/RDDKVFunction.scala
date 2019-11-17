package com.zy.sparkshizhan.deepsparkapi

import org.apache.spark.sql.SparkSession

object RDDKVFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("baseRdd")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val tranFile = sc.textFile("file:///g:/test_data/spark/ch04_data_transactions.txt")
    val tranData = tranFile.map(_.split("#"))
    val transByCust = tranData.map(tran=>(tran(2).toInt,tran))
    //print content
//    println(transByCust.keys.distinct().count())
    val sorted = transByCust.map(f=>{
        (f._1,1)
    }).reduceByKey((x,y)=>x+y).map(f=>{(f._2,f._1)}).sortByKey(false).collect()
    //map有分区性
    sorted.foreach(f=>{
      println("客户：" + f._2 + ",交易次数：" + f._1)
    })
    transByCust.countByKey()//不能排序了
  }
}
