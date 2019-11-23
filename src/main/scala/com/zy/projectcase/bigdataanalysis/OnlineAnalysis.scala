package com.zy.projectcase.bigdataanalysis

import com.google.gson.Gson
import com.zy.projectcase.realtimeprocessword.KafkaCommon
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object OnlineAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("wordStatistics")

    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val kafkaParams = KafkaCommon.generateParam(classOf[StringDeserializer],classOf [StringDeserializer],"wordstatics")
    val ssc = new StreamingContext(conf,Seconds(5))
    val sQLContext = new SQLContext(ssc.sparkContext)
    ssc.sparkContext.setLogLevel("WARN")

    val topics = Array("appstatics") //消费主题
    val stream = KafkaUtils.createDirectStream[String,String](ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val traned = stream.transform(rdd=>{

      rdd.map(line => {
        val gson = new Gson()
        val obj = gson.fromJson(line.value(),classOf[AppRequest])
        obj
      })
    })

    traned.foreachRDD(row =>{
      val struct = StructType(Array(
        StructField("appid",DataTypes.StringType,true),
          StructField("service",DataTypes.StringType,true),
        StructField("area",DataTypes.StringType,true),
        StructField("uid",DataTypes.StringType,true),
        StructField("dateTime",DataTypes.StringType,true),
        StructField("requestTime",DataTypes.IntegerType,true)
      ))
      val rd = row.map(app => Row(app.appid,app.service,app.area,app.uid,app.dateTime,app.requestTime))
      sQLContext.createDataFrame(rd,struct).registerTempTable("apptab")
      sQLContext.sql("select appid,max(requestTime),min(requestTime),avg(requestTime)" +
        " from apptab group by appid")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
