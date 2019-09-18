package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object foldByKeyRDD5 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val foldRDD: RDD[(Int, Int)] = arrRDD.foldByKey(0)(_+_)
    foldRDD.foreach(println(_))
  }
}
