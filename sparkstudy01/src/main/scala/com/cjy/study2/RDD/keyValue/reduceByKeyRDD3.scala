package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** reduceByKey(func: (V, V) => V, numPartitions: Int)
  * reduceByKey：在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，
  * numPartitions：任务的个数可以通过第二个可选的参数来设置。
  */
object reduceByKeyRDD3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[(String, Int)] = sc.parallelize(List(("female", 3), ("female", 1), ("male", 5), ("female", 5), ("male", 2), ("male", 1)))
    //(x, y) => x + y，相同key的两个值进行相加
    val reRDD: RDD[(String, Int)] = arrRDD.reduceByKey((x, y) => x + y,1)
//    val reRDD: RDD[(String, Int)] = arrRDD.reduceByKey(_+_,1)
    reRDD.foreach(println(_))

    /**
      * (male,8)
      * (female,9)
      */
  }
}
