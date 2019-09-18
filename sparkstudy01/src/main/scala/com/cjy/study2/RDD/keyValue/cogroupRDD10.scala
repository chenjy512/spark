package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object cogroupRDD10 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD1: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val arrRDD2: RDD[(Int, Int)] = sc.parallelize(Array((1,4),(2,5),(3,6)))
    val cogRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = arrRDD1.cogroup(arrRDD2)
    cogRDD.foreach(println(_))
  }
}
