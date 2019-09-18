package com.cjy.study2.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 2. RDD创建方式：在Spark中创建RDD的创建方式可以分为三种
  *               1. 从集合中创建RDD；
  *               2. 从外部存储创建RDD；
  *               3. 从其他RDD创建。-->RDD是不可变得，所以在处理数据过程中每次都是新建一个RDD，后续会着重介绍使用
  */
object CreateRDD1 {

  def main(args: Array[String]): Unit = {
    //    demo1
    //    demo2
    demo3
  }

  /**
    * 1. 从集合中创建：spark提供了两个函数：parallelize和makeRDD
    * 其实makeRDD函数底层还是调用了parallelize函数
    *
    */
  def demo1: Unit = {
    val conf = new SparkConf().setAppName("RDD_Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //两个创建函数
    val arrRDD1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val arrRDD2: RDD[Range.Inclusive] = sc.makeRDD(Array(11 to 20))

    arrRDD1.foreach(println(_))
    println("*****************")
    arrRDD2.foreach(println(_))
  }

  /**
    * 2. 从外部文件创建，这种方式比较常用
    */
  def demo2: Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Create")
    val sc = new SparkContext(conf)
    //读取指定路径下的文件，一次读取一行
    val fileRDD: RDD[String] = sc.textFile("in/a.txt")
    fileRDD.foreach(println(_))
  }

  /**
    * 3. 从其他RDD转换而来
    */
  def demo3: Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Create")
    val sc = new SparkContext(conf)
    //读取此文件数据，一次读取一行
    val fileRDD: RDD[String] = sc.textFile("in/a.txt")
    //读取的每行数据，按照 空格 分割
    val flatRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    flatRDD.foreach(println(_))
  }
}
