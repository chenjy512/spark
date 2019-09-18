package com.cjy.study1.wd

import org.apache.spark.{SparkConf,SparkContext}

/**
  *1. 手动编写一个wordcount程序
  */
object WordCount {

      def main(args: Array[String]): Unit = {
            // 创建配置，并设置任务名称，
            // setMaster("local[*]")：用于在机测试使用，当需要打包到spark环境下运行时不需要。
            val conf = new SparkConf().setAppName("WC---").setMaster("local[*]")

//            val conf = new SparkConf().setAppName("WC---") //打包提交到spark下运行时
            val sc = new SparkContext(conf)
            sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile(args(1))
//            sc.textFile("in/").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile("out/")
            sc.stop()
      }
}


