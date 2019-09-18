package com.cjy.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object SparkSQL_UDAF1 {

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
    val udaf = new MyAverage    //3.1 创建对象
    spark.udf.register("avgAge", udaf)  //3.2 注册对象
    val df = spark.read.json("data/user.json")
    df.createOrReplaceTempView("persons")
    spark.sql("select avgAge(age) from persons").show()  //3.3 使用聚合函数
    spark.stop()
    /**
      * +--------------+
      * |myaverage(age)|
      * +--------------+
      * |          21.0|
      * +--------------+
      */
  }
}

/**
  * 自定义聚合函数：弱类型DataFrame
  */
class MyAverage extends UserDefinedAggregateFunction {

  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算之前的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
