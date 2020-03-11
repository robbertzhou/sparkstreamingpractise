package com.zy.bigdataanalysis

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * create 2020-03-11
  * author zhouyu
  * desc jdbc rdd
  */
object MyJdbcTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mysqljdbc")
    val sc = new SparkContext(conf)
    val inputMysql = new JdbcRDD(sc,
      ()=>{
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://192.168.0.126:3306/testdb","root","mima")
      },
      "select * from person where id>=? and id<=?",
      1,3,
      1,
      r => (r.getInt(1),r.getString(2),r.getInt(3))
    )
    inputMysql.foreach(println)
    sc.stop()
  }
}
