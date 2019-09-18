package com.cjy.study2.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * countByKey：针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。
  */
object countByKey10 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("b", 1), ("c", 1), ("c", 1), ("c", 1), ("e", 1)))
    //统计每个key的个数
    val cRDD: collection.Map[String, Long] = arrRDD.countByKey()
    cRDD.foreach(println(_))
    /**
      * (e,1)
      * (a,2)
      * (b,1)
      * (c,3)
      */

    val tupleToLong: collection.Map[(String, Int), Long] = arrRDD.countByValue()
    tupleToLong.foreach(println(_))
    /**
      * ((b,1),1)
      * ((a,1),2)
      * ((e,1),1)
      * ((c,1),3)
      */
  }
}
