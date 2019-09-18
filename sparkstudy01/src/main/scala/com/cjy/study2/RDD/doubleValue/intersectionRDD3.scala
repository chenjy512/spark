package com.cjy.study2.RDD.doubleValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * intersection：对源RDD和参数RDD求交集后返回一个新的RDD
  */
object intersectionRDD3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val oneRDD: RDD[Int] = sc.parallelize(1 to 8)
    val twoRDD: RDD[Int] = sc.parallelize(4 to 10)
    val unit: RDD[Int] = oneRDD.intersection(twoRDD)
    unit.foreach(println(_))
    // 4、5、6、7、8
  }
}
