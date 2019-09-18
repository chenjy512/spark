package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap: 类似于map，每个元素都要执行，但是它接收的值是一个多元素的集合
  */
object flatMapRDD4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map partition").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(1 to 4)
    //    val fRDD = arrRDD.flatMap(_ * 2)  //这样返回一个结果的话就会报错
    val fRDD = arrRDD.flatMap(1 to _) //支持接收多个元素的集合
    fRDD.foreach(println(_))
  }
}
