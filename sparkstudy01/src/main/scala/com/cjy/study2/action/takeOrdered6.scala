package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}
/**
  * takeOrdered：返回该RDD排序后的前n个元素组成的数组
  */
object takeOrdered6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(Array(4, 5, 1, 3, 7, 3, 5, 1, 2))
    val ints: Array[Int] = arrRDD.takeOrdered(4)
    ints.foreach(println(_))
    /**
      * 1
      * 1
      * 2
      * 3
      */
  }
}
