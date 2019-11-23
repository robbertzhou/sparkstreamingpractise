package com.zy.projectcase.realtimeprocessword

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object UpdateByKeyApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
            conf.setMaster("local[2]")
    conf.setAppName("wordStatistics")

    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val kafkaParams = KafkaCommon.generateParam(classOf[StringDeserializer],classOf [StringDeserializer],"wordstatics")
    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("file:///i:/cppp")
    val topics = Array("words") //消费主题
    val stream = KafkaUtils.createDirectStream[String,String](ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val keyedStream = stream.transform(rdd=>{
      val fm = rdd.flatMap(record => record.value().split(","))
      fm.map(line => (line,1))
    }).updateStateByKey(update)
    keyedStream.foreachRDD(row=>{
      if(!row.isEmpty()){
        row.foreachPartition(itt=>{
          val iterator = itt.toIterator
          while (iterator.hasNext){
            println(iterator.next())
          }
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def update(values: Seq[Int],state:Option[Int]):Option[Int] = {
    val total = values.sum
    val current = state.getOrElse(0)
    Some(current + total)
  }
}
