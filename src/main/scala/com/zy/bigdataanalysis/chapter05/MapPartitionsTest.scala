package com.zy.bigdataanalysis.chapter05

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * create 2020-03-14
  * author zhouyu
  * desc mapPartition操作练习
  */
object MapPartitionsTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mapPartitionTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val rddData = spark.sparkContext.parallelize(Array(("201800001",83),("201800002",97),("201800003",100)
    ,("201800004",95),("201800005",87)),2)

    val rddData2 = rddData.mapPartitions(iter => {
        var result = List[String]()
      while (iter.hasNext){
        result = iter.next() match {
          case (id,grade) if grade >= 95 => {
            id + "_" + grade :: result
          }
          case _ => result
        }
      }
      result.toIterator
    })
    rddData2.foreach(println)
  }
}
