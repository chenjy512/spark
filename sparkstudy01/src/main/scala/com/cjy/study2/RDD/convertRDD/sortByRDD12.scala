package com.cjy.study2.RDD.convertRDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * sortBy：使用func先对数据进行处理，按照处理后的数据比较结果排序，默认为正序。
  */
object sortByRDD12 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map partition").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(List(4, 2, 5, 6, 1, 2,3,7))
    arrRDD.foreach(println(_))
    //按照元素本身大小排序
    val sortRDD: RDD[Int] = arrRDD.sortBy(x => x)
//    sortRDD.foreach(println(_))
    //按照指定规则排序，如下，按照 取模2的值排序
    val sRDD: RDD[Int] = arrRDD.sortBy(x => x % 2)
//    sRDD.foreach(println(_))
    //反序
    arrRDD.sortBy(x => x * -1).foreach(println(_))
  }
}
