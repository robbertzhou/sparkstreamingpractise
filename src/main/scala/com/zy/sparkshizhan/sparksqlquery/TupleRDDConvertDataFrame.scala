package com.zy.sparkshizhan.sparksqlquery

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SparkSession}
import StringImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
case class PostEntity(commentCount:Option[Int],lastActivityDate:Option[java.sql.Timestamp],ownerUserId:Option[Long],body:String,creationDate:Option[Int])

object TupleRDDConvertDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("baseRdd")
      .master("local[2]")
      .getOrCreate()
    val reader = spark.sparkContext.textFile("file:///g:/test_data/spark/italianPosts.csv")
    import spark.implicits._
    val rdd  = reader.map(line =>{
      val s = line.toString().replace("[","").replace("]","")
      stringToRow(s)
    })
    val stype = StructType(Seq(
      StructField("commentCount",DataTypes.IntegerType),
      StructField("lastActivityDate",DataTypes.TimestampType),
      StructField("ownerUserId",DataTypes.LongType),
      StructField("body",DataTypes.StringType),
      StructField("creationDate",DataTypes.IntegerType)
    ))
    val df = spark.createDataFrame(rdd,stype)

//        .map(x=>(x(0),x(1),x(2),x(3),x(4))).map(ele =>PostEntity(ele._1.toIntSafe,ele._2.toTimeStampSafe,
//      ele._3.toLongSafe,"body",ele._5.toIntSafe))
//    val df = rdd.toDF()
    df.show(20)
    df.printSchema()
  }

  def stringToRow(s:String) :Row = {
    val r = s.split("~")

    Row(r(0).toInt,Timestamp.valueOf(r(1)),r(2).toLong,r(3),r(4).toInt)
  }
}
