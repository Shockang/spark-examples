package com.shockang.study.spark.sql.read

import com.shockang.study.spark.{READ_DATA_DIR, printArray}
import org.apache.spark.sql.SparkSession

/**
 * 读取 txt 文件
 *
 * @author Shockang
 */
object ReadTxtDataFrameExample {
  def main(args: Array[String]): Unit = {

    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ReadTxtDataFrameExample").getOrCreate()

    // 读取 txt 文件
    val inputTextFile = spark.read.textFile(READ_DATA_DIR + "people.txt")

    // 结果日志输出
    printArray(inputTextFile.collect)

    // 读取目录下所有 txt 文本文件
    val allTextFile = spark.read.textFile(READ_DATA_DIR + "*.txt")

    // 结果日志输出
    printArray(allTextFile.collect)

    // 停止sc，结束该任务
    spark.stop()
  }
}
