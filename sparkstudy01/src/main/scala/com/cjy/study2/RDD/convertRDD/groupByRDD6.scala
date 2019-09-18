package com.cjy.study2.RDD.convertRDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 传入函数，按照函数返回值进行分组
  */
object groupByRDD6 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map partition").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(1 to 16)
    //    val groupRDD: RDD[(Int, Iterable[Int])] = arrRDD.groupBy(_ % 2)
    //调用自定义分组函数
    val groupRDD: RDD[(Int, Iterable[Int])] = arrRDD.groupBy(dealPartition(_))
    groupRDD.foreach(println(_))

    /**
      * (0,CompactBuffer(2, 4, 6, 8, 10, 12, 14, 16))
      * (1,CompactBuffer(1, 3, 5, 7, 9, 11, 13, 15))
      *
      * -------
      * (0,CompactBuffer(1, 2, 3, 4, 5))
      * (1,CompactBuffer(6, 7, 8, 9, 10))
      * (2,CompactBuffer(11, 12, 13, 14, 15, 16))
      */
  }

  /**
    * 按照大小进行分区
    * @param p
    * @return
    */
  def dealPartition(p: Int): Int = {
    if (p <= 5) {
      0
    } else if (p > 5 && p <= 10) {
      1
    } else {
      2
    }
  }
}
