package com.zy.projectcase.realtimeprocessword


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import com.zy.JiebaGetter

object WordStatisticsApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//    conf.setMaster("local[2]")
    conf.setAppName("wordStatistics")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val kafkaParams = KafkaCommon.generateParam(classOf[StringDeserializer],classOf [StringDeserializer],"wordCount")
    val ssc = new StreamingContext(conf,Duration.apply(3000))

    val topics = Array("wordCount") //消费主题
    val stream = KafkaUtils.createDirectStream[String,String](ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    stream.foreachRDD(f=>{
      if(!f.isEmpty()){
        // offset
        val offsetRanges = f.asInstanceOf[HasOffsetRanges].offsetRanges
        f.foreachPartition(row=>{
          if(!row.isEmpty){
            val it = row.toIterator
            while (it.hasNext){
              val words = it.next().value()

              val segmenter = JiebaGetter.getInstance().getJieba;
              println(segmenter.sentenceProcess(words))

            }
          }
        })
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
