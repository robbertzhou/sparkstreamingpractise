package com.zy.bizfight.graphxtest

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx.{Graph, GraphLoader, PartitionID}
import org.apache.spark.sql.SparkSession

object GraphxWebgoogle {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("graphxcode").getOrCreate()
    val sc = spark.sparkContext
    val graphFiles : Graph[PartitionID,PartitionID] =
      GraphLoader.edgeListFile(sc,"file:///i:/testdata/web-Google.txt",numEdgePartitions = 4)

    println("vertex.count=" + graphFiles.vertices.count())
    println("vertex.edges.count=" + graphFiles.edges.count())
  }
}
