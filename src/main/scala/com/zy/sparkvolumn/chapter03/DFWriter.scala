package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil
import org.apache.spark.sql.SaveMode

/**
  * create 2020-02-08
  * author zhouyu
  * desc dataframe写入练习
  */
object DFWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    spark.conf.set("spark.sql.ORC.impl","native")
    val df = spark.read.json("file:///I:\\testdata\\grade.json")
    //写出json文件
    df.select("grade","name").write.json("file:///i:/testdata/writejson.json")
    //写出parquet文件
    df.select("grade","name").write.parquet("file:///i:/testdata/writeparquet.parquet")
    //写出orc文件
//    df.select("grade","name").write.orc("file:///i:/testdata/writeorc.orc")
    //写出text
    df.select("name").write.text("file:///i:/testdata/writetext.txt")

    import spark.implicits._
    //写出csv文件
    val saveOptions = Map("header"->true,"path" -> "csvout")
//    df.select("age","name").write
//      .format("com.databricks.spark.csv")
//      .mode(SaveMode.Overwrite)
//      .save()

  }
}
