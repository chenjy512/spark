package com.cjy.study2.funcd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 传递函数：在实际开发中我们往往需要自己定义一些对于RDD的操作，那么此时需要注意的是，初始化工作是在Driver端进行的，
  * 而实际运行程序是在Executor端进行的，这就涉及到了跨进程通信，是需要序列化的
  */
object SeriTest{
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //3.创建一个Search对象
    val search = new Search()
    //4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatch1(rdd)
    match1.collect().foreach(println)
  }
}

class Search extends Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains("h")
  }
  //过滤出包含字符串的RDD
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    //传入一个函数
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    //直接编写匿名函数,过滤RDD中的数据返回一个新的RDD
    rdd.filter(x => x.contains("h"))
  }


}
