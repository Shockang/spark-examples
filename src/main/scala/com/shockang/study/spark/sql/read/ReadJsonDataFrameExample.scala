package com.shockang.study.spark.sql.read

import com.shockang.study.spark._
import org.apache.spark.sql.SparkSession

/**
 * 读取 json 文件
 *
 * @author Shockang
 */
object ReadJsonDataFrameExample {

  def main(args: Array[String]): Unit = {

    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ReadJsonDataFrameExample").getOrCreate()

    // 读取文本文件
    val inputJsonFile = spark.read.json(READ_DATA_DIR + "employees.json")

    printArray(inputJsonFile.collect())
    // 停止 spark，结束该任务
    spark.stop()
  }
}
