package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapValues: 针对于(K,V)形式的类型只对V进行操作
  */
object mapValuesRDD8 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    val mapRDD: RDD[(Int, String)] = arrRDD.mapValues(_+"|||")
    mapRDD.foreach(println(_))
  }
}
