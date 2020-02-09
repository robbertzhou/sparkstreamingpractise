package com.zy.sparkvolumn.chapter03

import com.zy.sparkvolumn.SparkUtil

/**
  * create 2020-02-09
  * author zhouyu
  * desc 创建dataset的临时视图
  */
object CreateTempView {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSession
    spark.conf.set("spark.sql.ORC.impl","native")
    val df = spark.read.json("file:///I:\\testdata\\grade.json")
    import spark.implicits._

    val maped = df.map(a=>{
      val name = a.getAs[String]("name")
      val subject = a.getAs[String]("subject")
      val grade = a.getAs[Long]("grade")
      new StudentGrade(name,subject,grade)
    })
    //create temp view
    val dsTemp = maped.createOrReplaceTempView("grade_ds")
    val dfTemp = df.createOrReplaceTempView("grade_df")
    //计算平均分
    spark.sql("select subject,avg(grade) from grade_ds group by subject").show()
    spark.sql("select subject,avg(grade) from grade_df group by subject ").show()
  }
}
