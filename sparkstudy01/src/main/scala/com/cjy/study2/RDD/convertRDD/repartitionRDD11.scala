package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * repartition：根据分区数，重新通过网络随机洗牌所有数据
  *
  *             底层其实是调用coalesce，进行shuffle
  */
object repartitionRDD11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map partition").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(1 to 16,4)
//    arrRDD.foreach(println(_))

    val reRDD: RDD[Int] = arrRDD.repartition(2)
    reRDD.foreach(println(_))
  }
}
