package com.zy.sparkvolumn.chapter03

/**
  * create 2020-02-08
  * author zhouyu
  * desc 对成绩的udf函数
  */
class GradeUdf extends Serializable {
  def level(grade:Int): String ={
    if(grade >= 85){
      "A"
    }else if(grade < 85 && grade >= 75){
      "B"
    }else if(grade < 75 && grade >= 60){
      "C"
    }else if(grade < 60){
      "D"
    }else{
      "ERROR"
    }
  }
}
