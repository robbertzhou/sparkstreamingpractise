package com.zy.sparkvolumn.chapter02

import com.zy.sparkvolumn.SparkUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
  * create 2020-02-07
  * author zhouyu
  * desc 从hbase源创建RDD，两种方式：一是scan操作，另一种是直接读取region
  */
object HbaseSource {
  def main(args: Array[String]): Unit = {
    val session = SparkUtil.getSession
    val sc = session.sparkContext
    val tableName = "movie"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","master.zy.com")
    conf.set("hbase.zookeeper.property.clientPort","2181")
    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    val rdd_three = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],classOf[Result])
    rdd_three.foreach(row=>{

      val result = row._2
      val rk = new String(result.getRow)
      val movieName = new String(result.getValue("analyse".getBytes(),"movie_name".getBytes()))
      val rating = new String(result.getValue("analyse".getBytes(),"rating".getBytes()))
      println("name=" + movieName + ",rating=" + rating)
    })
  }
}
