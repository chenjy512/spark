package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupByKey：也是对每个key进行操作，但只生成一个sequence。
  */
object groupByKeyRDD2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[String] = sc.parallelize(Array("one", "two", "two", "three", "three", "three"))
    val mapRDD: RDD[(String, Int)] = arrRDD.map((_, 1))
    val gRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    //    gRDD.foreach(println(_))
    val m2RDD: RDD[(String, Int)] = gRDD.map(t => (t._1, t._2.sum))
    m2RDD.foreach(println(_))
  }
}
