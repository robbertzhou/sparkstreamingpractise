package com.zy

import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object SendMsg {
  def BROKER_LIST = "slave1.zy.com:9092,slave2.zy.com:9092"
  def TOPIC = "kafka_test_4"
  def main(args: Array[String]): Unit = {

  }

  val props_1 = new Properties()
//  props_1.put("metadata.broker.list", BROKER_LIST)
  props_1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
  props_1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props_1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  def produceMsg(topic:String,msg:String): Unit ={

//    println("开始产生消息！")
    val props = new Properties()
    props.put("metadata.broker.list", BROKER_LIST)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//    props.put("security.protocol", "SASL_PLAINTEXT");
//    props.put("sasl.mechanism", "GSSAPI");
//    props.put("sasl.kerberos.service.name", "kafka")
//    props.put("java.security.auth.login.config","jaas.conf")
//    props.put("java.security.krb5.conf", "c:/krb5.ini")
//    java.security.auth.login.config=/home/kafka/kafka_client_jaas.conf   -Djava.security.krb5.conf=/home/kafka/krb5.conf

    val producer = new KafkaProducer[String, String](props)

    producer.send(new ProducerRecord(topic, "", msg))
    producer.close
  }

  def produceMap(topic:String,msg:util.List[util.HashMap[String,Array[Byte]]]): Unit ={
    println("开始产生消息！")
    val props = new Properties()
    props.put("metadata.broker.list", BROKER_LIST)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, util.List[util.HashMap[String,Array[Byte]]]](props)

    producer.send(new ProducerRecord(topic, "", msg))
    producer.close
  }

}
