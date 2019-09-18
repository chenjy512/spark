package com.cjy.spark.sql

import org.apache.spark.sql.{Dataset, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator


object SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf()并设置App名称，
    // 设置本地运行方式，一般用于本地测试，打包部署在spark环境下时不用设置。
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //2. 导入spark对象 For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    //3. 使用自定义聚合函数
    val udaf = new MyAgeAvgClassFunction //3.1 创建对象
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    val df = spark.read.json("data/user.json")
    val userDS: Dataset[UserBean] = df.as[UserBean]
    userDS.select(avgCol).show()
    spark.stop()
  }
}


/**
  * 强类型聚合函数：dataset
  *
  * @param name
  * @param age
  */
case class UserBean(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Int)

class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {

  //初始化缓冲区
  override def zero = {
    AvgBuffer(0L, 0)
  }

  //数据输入--更新缓冲区数据
  override def reduce(b: AvgBuffer, a: UserBean) = {
    b.count = b.count + 1
    b.sum = b.sum + a.age
    b
  }

  //合并各个节点的缓冲数据-- 聚合不同execute的结果
  override def merge(b1: AvgBuffer, b2: AvgBuffer) = {
    //合并缓冲数据
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    //返回合并后的数据
    b1
  }

  //完成---计算输出
  override def finish(reduction: AvgBuffer) = {
    reduction.sum.toDouble / reduction.count
  }

  // 设定之间值类型的编码器，要转换成case类
  // Encoders.product是进行scala元组和case类转换的编码器
  override def bufferEncoder = Encoders.product

  // 设定最终输出值的编码器
  override def outputEncoder = Encoders.scalaDouble
}