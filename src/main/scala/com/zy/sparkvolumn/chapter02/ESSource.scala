package com.zy.sparkvolumn.chapter02

import com.zy.sparkvolumn.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * create 2020-02-07
  * author zhouyu
  * desc 从es数据源读取数据,es是分布式采取水平（sharding）+副本（replication）
  */
object ESSource {
  def main(args: Array[String]): Unit = {
//    val spark = SparkUtil.getSession
//    val sc = spark.sparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("es.index.auto.create","true")
    sparkConf.set("es.nodes","master.zy.com")
    sparkConf.set("es.port","9200")
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("fromes")
    val sc = new SparkContext(sparkConf)
    val rdd = EsSpark.esRDD(sc,"auditindex")
    rdd.foreach(line =>{
      //key,id
      val key = line._1
      println(key)
    })
  }
}
