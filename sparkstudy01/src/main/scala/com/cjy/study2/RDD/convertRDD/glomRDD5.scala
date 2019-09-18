package com.cjy.study2.RDD.convertRDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * glom：将每一个分区形成一个数组
  */
object glomRDD5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map partition").setMaster("local[1]")
    val sc = new SparkContext(conf)
    //创建4个分区，将数据放置到4个数组中
    val arrRDD: RDD[Int] = sc.parallelize(1 to 16,4)
    //调用glom函数后，类型发生变化，泛型类型
    val gRDD: RDD[Array[Int]] = arrRDD.glom()
    val cRDD: Array[Array[Int]] = gRDD.collect()
    println(cRDD.length)   //4
    //高阶函数调用,遍历
    cRDD.foreach(x=> x.foreach(println))
  }
}
