package com.cjy.study2.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，
  * Spark将会调用toString方法，将它装换为文件中的文本
  */
object saveAsTextFile9 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("local-demo")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD = sc.makeRDD(1 to 10)
    //将数据放入文件中，一个元素一行
    arrRDD.saveAsTextFile("out")
  }
}
