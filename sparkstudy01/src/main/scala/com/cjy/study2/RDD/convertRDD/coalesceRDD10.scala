package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * coalesce：缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
  */
object coalesceRDD10 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(1 to 16, 4)
    println(arrRDD.partitions.size) //目前4个分区
    val coalRDD: RDD[Int] = arrRDD.coalesce(2)
    println(coalRDD.partitions.size)

  }
}
