package com.cjy.spark.sql

import org.apache.spark.sql.SparkSession

object HelloWorld {
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

    //3. 加载数据文件
    val df = spark.read.json("data/user.json")

    // Displays the content of the DataFrame to stdout
    //4. 展示数据
    df.show()

    df.filter($"age" > 21).show()
    //5. 创建临时视图
    df.createOrReplaceTempView("persons")
    //6. 根据临时视图查询数据
    spark.sql("SELECT * FROM persons where age > 21").show()

    spark.sql("select avg(age) from persons").show()
    spark.sql("select age,count(*) from persons group by age").show()
    spark.stop()
  }
}
