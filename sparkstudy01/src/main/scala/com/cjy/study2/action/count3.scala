package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * count：返回RDD中元素的个数
  */
object count3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10)
    val res: Long = arrRDD.count()
    println(res)
  }
}
