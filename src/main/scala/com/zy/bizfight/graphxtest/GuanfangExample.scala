package com.zy.bizfight.graphxtest

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GuanfangExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("graphxcode").getOrCreate()
    val sc = spark.sparkContext
    val users : RDD[(VertexId,(String,String))] = sc.parallelize(Array((3L,("rxin","student")),
      (7L,("jgonzal","postdoc")),
      (5L,("franklin","prof")),(2L,("istoica","prof"))))
    val relationships = sc.parallelize(Array(Edge(3L,7L,"collab"),
      Edge(5L,3L,"advisor"),Edge(2L,5L,"colleague")
    ,Edge(5L,7L,"PI")))
    val graph = Graph(users,relationships)
  }
}
