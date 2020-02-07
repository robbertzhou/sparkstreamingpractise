package com.zy.sparkvolumn.chapter02

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * create 2020-02-07
  * author zhouyu
  * desc 从mysql源创建RDD
  */
object JdbcMysqlSource {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("es.index.auto.create","true")
    sparkConf.set("es.nodes","master.zy.com")
    sparkConf.set("es.port","9200")
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("fromes")
    val sc = new SparkContext(sparkConf)
    val rdd = new JdbcRDD(sc,()=>{
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.0.126:3306/sakila","root","mima")
    },
    "select * from actor where actor_id > ? and actor_id < ?",
      1,1000,10,r => r.getString(1))
    rdd.foreach(println)
  }
}
