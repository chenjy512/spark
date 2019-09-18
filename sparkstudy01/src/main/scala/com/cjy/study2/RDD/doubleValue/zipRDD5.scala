package com.cjy.study2.RDD.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * zip：将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
  */
object zipRDD5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.parallelize(1 to 3, 3)
    val strRDD: RDD[String] = sc.parallelize(List("a", "b", "c"), 3)
    //合并元素
    val unit: RDD[(Int, String)] = intRDD.zip(strRDD)
    unit.foreach(println(_))

    /**
      * (3,c)
      * (2,b)
      * (1,a)
      */
  }
}
