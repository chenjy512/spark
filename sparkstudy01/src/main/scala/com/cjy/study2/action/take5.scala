package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * take(n)：返回一个由RDD的前n个元素组成的数组
  */
object take5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10)
    //取其前4个元素，组成新数组
    val ints: Array[Int] = arrRDD.take(4)
    ints.foreach(println(_))
  }
}
