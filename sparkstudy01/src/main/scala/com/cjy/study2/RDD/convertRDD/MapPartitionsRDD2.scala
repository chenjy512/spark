package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsRDD2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map partition").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(1 to 10)
    arrRDD.foreach(println(_))
    val pRDD = arrRDD.mapPartitions(x => x.map(_ * 2))
    pRDD.foreach(println)
  }
}

object Test {
  def main(args: Array[String]) {
    delayed(time());
  }

  def time() = {
    println("获取时间，单位为纳秒")
    System.nanoTime
  }
  def delayed( t: => Long ) = {
    println("在 delayed 方法内")
    println("参数： " + t)
    t
  }
}
