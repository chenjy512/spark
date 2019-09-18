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
//            sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile(args(1))
            sc.textFile("in/").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile("out/")
        /**
          * 步骤解析：
          * textFile：读取一行行数据
          * flatMap：每行数据按照空格符号分割
          * map：转换数据形态，hello-->(hello,1)
          * reduceByKey：将相同key聚集在一起，并计算v的值，第二个参数表示使用几个分区，聚合效果：(hello,1),(hello,1)-->(hello,2)
          * sortBy：排序，按照v大小排序，false 降序排序
          * saveAsTextFile：保存到本地文件，当前工程下的out文件夹下面，切记此文件夹不能存在，否则运行报错
          */
            sc.stop()
      }
}


