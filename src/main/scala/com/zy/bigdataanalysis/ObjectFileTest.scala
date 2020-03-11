package com.zy.bigdataanalysis

import org.apache.spark.{SparkConf, SparkContext}

/**
  * create 2020-03-11
  * author zhouyu
  * desc
  */
object ObjectFileTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("object")
    val sc = new SparkContext(conf)
    val src = sc.parallelize(Array(Person("张三",11),Person("lis",22)))
    src.saveAsObjectFile("file:///i:/testdata/person.object")
    sc.stop()
  }
}

case class Person(name:String,age:Int)
