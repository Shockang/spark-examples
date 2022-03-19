package com.shockang.study.spark.sql.read

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 读取 csv/tsv 文件
 *
 * @author Shockang
 */
object ReadCollectionDataFrameExample {
  def main(args: Array[String]): Unit = {

    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ReadCsvDataFrameExample").getOrCreate()

    val df1 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    df1.show()

    val schema = StructType(List(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("sex", StringType, nullable = true)
    ))

    val javaList = new java.util.ArrayList[Row]()
    javaList.add(Row("Alice", 20, "Female"))
    javaList.add(Row("Tom", 18, "Male"))
    javaList.add(Row("Boris", 30, "Male"))
    val df2 = spark.createDataFrame(javaList, schema)
    df2.show

    // 停止sc，结束该任务
    spark.stop()
  }
}
