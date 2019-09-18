package com.cjy.study2.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。
  */
object reduce1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("local-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10)
    val res: Int = arrRDD.reduce((x, y) => x + y)
    println(res)

    val arrRDD2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3), ("d", 4)))
    val res2: (String, Int) = arrRDD2.reduce((x, y) => (x._1 + y._1, x._2 * y._2))
    println(res2)
  }
}
