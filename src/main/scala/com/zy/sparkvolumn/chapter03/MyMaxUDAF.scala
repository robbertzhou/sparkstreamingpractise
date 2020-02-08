package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._
/**
  * create 2020-02-08
  * author zhouyu
  * desc 实现某列最大值得UDAF，用户自定义函数
  */
class MyMaxUDAF extends UserDefinedAggregateFunction{
  //指定输入类型
  override def inputSchema: StructType = StructType(Array(StructField("input",IntegerType,true)))

  //指定中间输出类型，可指定多个
  override def bufferSchema: StructType = StructType(Array(StructField("input",IntegerType,true)))

  //指定最终输出类型
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  //初始化中间结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = (buffer(0) == 0)

  //实现作用在每个分区的结果
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val temp = input.getAs[Int](0)
    val current = buffer.getAs[Int](0)
    if(temp > current){
      buffer(0) = temp
    }
  }

  //合并多个分区的结果
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.getAs[Int](0) < buffer2.getAs[Int](0)){
      buffer1(0) = buffer2.getAs[Int](0)
    }
  }

  //返回最终的结果
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}

/**
  * 测试
  */
object MyMaxUDAF{
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    val df = spark.read.json("file:///I:\\testdata\\grade.json")
    val udf = new MyMaxUDAF()
    df.select(udf(col("grade"))).show()
  }
}
