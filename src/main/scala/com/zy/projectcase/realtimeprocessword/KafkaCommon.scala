package com.zy.projectcase.realtimeprocessword

object KafkaCommon {
  def generateParam[T](keyCls:Predef.Class[T],valueCls:Predef.Class[T],consumerGroup:String): Map[String,Object] ={
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Config.KAFKA_BOOT_SERVER, //kafka集群地址
      "key.deserializer" -> keyCls,
      "value.deserializer" -> valueCls,
      "fetch.message.max.bytes" -> "10485760",
      "group.id" -> consumerGroup, //消费者组名
      "auto.offset.reset" -> "earliest", //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      "enable.auto.commit" -> (true: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    kafkaParams
  }
}
