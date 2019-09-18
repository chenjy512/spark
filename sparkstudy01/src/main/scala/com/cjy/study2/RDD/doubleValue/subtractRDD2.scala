package com.cjy.study2.RDD.doubleValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * subtract：计算差的一种函数，去除两个RDD中的相同元素，保留调用RDD剩下的元素。
  */
object subtractRDD2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val oneRDD: RDD[Int] = sc.parallelize(1 to 8)
    val twoRDD: RDD[Int] = sc.parallelize(4 to 10)
    //去掉相同元素，保留oneRDD剩下元素
    val unit: RDD[Int] = oneRDD.subtract(twoRDD)
    unit.foreach(println(_))

    /**
      * 1
      * 3
      * 2
      */
  }
}
