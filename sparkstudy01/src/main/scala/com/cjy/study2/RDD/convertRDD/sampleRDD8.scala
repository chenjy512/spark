package com.cjy.study2.RDD.convertRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sample(withReplacement, fraction, seed): 以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。
  *
  * seed：随机种子一样其实抽取出来的数据结果不会变，因为它是按照此随机种子根据一定规则抽取数据，所以规则与种子不变，结果不会变。
  */
object sampleRDD8 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("t_demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val arrRDD: RDD[Int] = sc.parallelize(1 to 10)
    val sampleRDD: RDD[Int] = arrRDD.sample(false, 0.2, 1)
    sampleRDD.foreach(println(_))
  }
}
