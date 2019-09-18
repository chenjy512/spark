package com.cjy.study2.RDD.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * cartesian：笛卡尔积（尽量避免使用）
  */
object cartesianRDD4 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val oneRDD: RDD[Int] = sc.parallelize(1 to 3)
    val twoRDD: RDD[Int] = sc.parallelize(2 to 5)

    val carRDD: RDD[(Int, Int)] = oneRDD.cartesian(twoRDD)
    carRDD.foreach(println(_))

  }
}
