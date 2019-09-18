package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * collect：在驱动程序中，以数组的形式返回数据集的所有元素。
  */
object collect2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10)
    val ints: Array[Int] = arrRDD.collect()
  }
}
