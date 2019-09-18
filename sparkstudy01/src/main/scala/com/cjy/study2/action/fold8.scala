package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}

/*
 *fold：折叠操作，aggregate的简化操作，seqop和combop一样。
 */
object fold8 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10, 2)
    val res: Int = arrRDD.fold(0)(_+_)
    println(res)
  }
}
