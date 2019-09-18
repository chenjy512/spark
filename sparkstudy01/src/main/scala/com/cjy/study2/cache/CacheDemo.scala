package com.cjy.study2.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("t-demo").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arrRDD = sc.makeRDD(Array("hello"))
    //携带当前时间戳不做缓存
    val mapRDD = arrRDD.map(_.toString+System.currentTimeMillis())
//    mapRDD.foreach(println(_))
//    mapRDD.foreach(println(_))
    //做了缓存，后面几次打印输出的结果一样。
    val cacheRDD: RDD[String] = arrRDD.map(_.toString+System.currentTimeMillis()).cache()
    cacheRDD.foreach(println(_))
    cacheRDD.foreach(println(_))
    cacheRDD.foreach(println(_))
  }
}
