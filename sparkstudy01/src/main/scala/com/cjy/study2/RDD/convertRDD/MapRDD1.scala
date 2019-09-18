package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * map(func)： 每个元素经过 func 函数处理后转换成，组合成一个新的RDD 返回
  */
object MapRDD1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("value-rdd").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.parallelize(1 to 10)
    //每个元素 * 2 返回一个新的rdd
//    val mapRDD: RDD[Int] = arrRDD.map(_ * 2)
    //使用自定义函数
    val editRDD = arrRDD.map(editMap(_))
//    mapRDD.foreach(println(_))
    editRDD.foreach(println(_))
    sc.stop()
  }

  //每个元素后面加上一句话返回
  def editMap(p : Int):String={
    p+"-自定义函数"
  }
}
