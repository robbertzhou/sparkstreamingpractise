package com.zy.bizfight.ch01

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  */
object RDDMovie_Users_Analyzier {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("userAnaly")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val dataPath = ""
    val userRdd = sc.textFile(dataPath +"")
    val movieRdd = sc.textFile(dataPath + "")
    val ratings = sc.textFile(dataPath + "")


  }
}
