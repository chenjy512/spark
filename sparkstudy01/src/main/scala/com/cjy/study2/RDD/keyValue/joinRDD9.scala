package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * join: 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
  */
object joinRDD9 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD1: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val arrRDD2: RDD[(Int, Int)] = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))
    val joinRDD: RDD[(Int, (String, Int))] = arrRDD1.join(arrRDD2)
    joinRDD.foreach(println(_))

    /**
      * (1,(a,4))
      * (3,(c,6))
      * (2,(b,5))
      */
  }
}
