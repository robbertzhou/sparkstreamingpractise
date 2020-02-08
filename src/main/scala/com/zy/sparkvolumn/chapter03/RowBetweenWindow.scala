package com.zy.sparkvolumn.chapter03



import com.zy.sparkvolumn.SparkUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number,sum}

/**
  * create 2020-02-08
  * author zhouyu
  * desc 窗口函数的行控制
  */
object RowBetweenWindow {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    import spark.implicits._
    val df = spark.read.json("file:///I:\\testdata\\window.json")
    val window = Window.partitionBy("key").orderBy("num")
        .rangeBetween(Window.currentRow  +2,Window.currentRow+20)
    df.select(col("key"),col("num"),sum("num").over(window)).sort("key").show()
  }
}
