package com.cjy.study2.RDD.convertRDD

import org.apache.spark.{SparkConf, SparkContext}

object mapPartitionsWithIndexRDD3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map partition").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arrRDD = sc.parallelize(1 to 4)
    val pRDD = arrRDD.mapPartitionsWithIndex((index,items) => (items.map((index,_))))
    pRDD.foreach(println)
  }
}
