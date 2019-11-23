package com.zy.projectcase.bigdataanalysis

/**
  * app请求日期
  */
class AppRequest extends Serializable {
  //请求app id号
  var appid = ""
  //请求服务名称
  var service = ""
  //客户端身份名称
  var area = ""
  //用户id
  var uid = ""
  //请求时间
  var dateTime = ""
  //请求花费时间
  var requestTime : Integer = 0

}
