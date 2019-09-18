package com.cjy.study2.RDD.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * union：对源RDD和参数RDD求并集后返回一个新的RDD---->并集
  */
object unionRDD1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val oneRDD: RDD[Int] = sc.parallelize(1 to 5)
    val twoRDD: RDD[Int] = sc.parallelize(6 to 10)

    val unit: RDD[Int] = oneRDD.union(twoRDD)
    unit.foreach(println(_))

  }
}
