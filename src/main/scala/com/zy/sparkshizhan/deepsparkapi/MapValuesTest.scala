package com.zy.sparkshizhan.deepsparkapi

object MapValuesTest {
  def main(args: Array[String]): Unit = {
    val transByCust = Common.getTransCust()
//    transByCust.mapValues(tran =>{
//      tran(2)=String.valueOf(tran(2).toInt * 40)
//      tran.mkString(",")
//    }).foreach(println)
    val len = transByCust.flatMapValues(tran=>{
      if(tran(3).toInt == 81 && tran(4).toDouble>=5){
        val cloned = tran.clone()
        cloned(5)="0.00"
        cloned(3) = "70"
        cloned(4) = "1"
        List(tran,cloned)
      }else{
        List(tran)
      }
    }).count()
    println("row len is " + len)
  }
}
