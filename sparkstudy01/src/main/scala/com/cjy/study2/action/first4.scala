package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * first：返回RDD中的第一个元素
  */
object first4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10)

    println(arrRDD.sortBy(x => x * -1).first())
  }
}
