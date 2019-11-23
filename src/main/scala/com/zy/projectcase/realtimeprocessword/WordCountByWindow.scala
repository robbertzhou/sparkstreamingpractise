package com.zy.projectcase.realtimeprocessword

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object WordCountByWindow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//        conf.setMaster("local[2]")
    conf.setAppName("wordStatistics")

    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val kafkaParams = KafkaCommon.generateParam(classOf[StringDeserializer],classOf [StringDeserializer],"wordstatics")
    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("hdfs://master.zy.com:8020/data/cp")
    val topics = Array("words") //消费主题
    val stream = KafkaUtils.createDirectStream[String,String](ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    stream.transform(rdd=>{
      val fm = rdd.flatMap(record => record.value().split(","))
      fm.map(line => (line,1))
    }).reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(10), Seconds(5)).print()
//    stream.foreachRDD(rdd=>{
//      if(!rdd.isEmpty()){
//        rdd.foreachPartition(it=>{
//          val iterator = it.toIterator
//        })
//      }
//    })
    ssc.start()
    ssc.awaitTermination()
  }
}
