package com.zy.projectcase.bigdataanalysis

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.Gson
import com.zy.SendMsg

import scala.collection.mutable
import scala.util.Random

object SendAppRequest {
  val appid = Array("百度","阿里","腾讯")
  val services = new mutable.HashMap[String,Array[String]]()
  val baidu= Array("百度搜索","百度搜索")
  services.put("百度",baidu)
  val ali = Array("淘宝","支付宝")
  services.put("阿里",ali)
  val tx = Array("qq","微信")
  services.put("腾讯",tx)

  val provinces = Array("北京","上海","广州","深圳")
  val users = Array("001","002","003","004")

  def main(args: Array[String]): Unit = {
    val random = new Random()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    while(true){
        val ar = new AppRequest
        ar.appid = appid.apply(random.nextInt(appid.size))
      val appName = ar.appid
      val list : Option[Array[String]] = services.get(appName)
      val aaa = list.get
      if(aaa!=null){
        ar.service =  aaa.apply(random.nextInt(aaa.size))
      }
      ar.area = provinces(random.nextInt(4))
      ar.dateTime = sdf.format(new Date())
      ar.uid = users.apply(random.nextInt(4))
      ar.requestTime = random.nextInt(1000)

      val gson = new Gson()
      val str = gson.toJson(ar)
      SendMsg.produceMsg("appstatics",str)
        Thread.sleep(20)
      }
  }
}
