package com.cjy.study2.RDD.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * partitionBy：对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区，
  * 否则会生成ShuffleRDD，即会产生shuffle过程。
  *
  */
object partitionByRDD1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    val pRDD: RDD[(Int, String)] = arrRDD.partitionBy(new org.apache.spark.HashPartitioner(2))
    pRDD.foreach(println(_))
  }
}
