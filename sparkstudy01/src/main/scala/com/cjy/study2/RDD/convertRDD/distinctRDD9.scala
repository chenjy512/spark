package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * distinct：对源RDD进行去重后返回一个新的RDD。默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。
  */
object distinctRDD9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(Array(1,2,3,2,1,5,4,3,4,2,5,1,2))
//    val disRDD: RDD[Int] = arrRDD.distinct()
    val disRDD: RDD[Int] = arrRDD.distinct(2)
    disRDD.foreach(println(_))
  }
}
