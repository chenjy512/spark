package com.cjy.study2.RDD.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combineByKeyRDD6 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val arrRDD: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    val comRDD: RDD[(String, (Int, Int))] = arrRDD.combineByKey((_,1), (acc:(Int,Int), v)=>(acc._1+v,acc._2+1), (acc1:(Int,Int), acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    val mapRDD: RDD[(String, Double)] = comRDD.map{case (key,value) => (key,value._1/value._2.toDouble)}
    mapRDD.foreach(println(_))
  }
}
