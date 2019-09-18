package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKeyRDD4 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    //取出每个分区相同key对应值的最大值，然后相加，每个key初始默认值为0
    val aggRDD: RDD[(String, Int)] = arrRDD.aggregateByKey(0)(Math.max(_,_),_+_)
    aggRDD.foreach(println(_))
  }
}
