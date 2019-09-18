package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregate：函数将每个分区里面的元素通过seqOp和初始值进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
  * 这个函数最终返回的类型不需要和RDD中元素类型一致。
  */
object aggregate7 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("t-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10,2)
    //现在分区内相加，最后汇总多个结果
    val res: Int = arrRDD.aggregate(0)(_+_, _+_)
    println(res)
  }
}
