package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * filter：按照指定函数的过滤后返回新的RDD，新的RDD根据函数的返回结果是true的保留元素组成
  */
object filterRDD7 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[String] = sc.parallelize(Array("xiaoming", "xiaojiang", "xiaohe", "dazhi"))
    //满足规则的保留，并形成新的RDD
    val filterRDD: RDD[String] = arrRDD.filter(_.indexOf("xiao") != -1)
    filterRDD.foreach(println(_))
  }
}
