package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sortByKeyRDD7 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    //正序排序--小到大
    arrRDD.sortByKey(true).foreach(println(_))
    println("***********************************")
    //反序排序--大到小
    arrRDD.sortByKey(false).foreach(println(_))
  }
}
