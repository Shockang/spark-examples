package com.shockang.study.spark.sql.read

import com.shockang.study.spark._
import org.apache.spark.sql.SparkSession

/**
 * 读取 csv/tsv 文件
 *
 * @author Shockang
 */
object ReadHadoopDataFrameExample {
  def main(args: Array[String]): Unit = {

    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ReadCsvDataFrameExample").getOrCreate()
    // 读取 csv 文件
    val inputCsvFile = spark.read.csv(READ_DATA_DIR + "people.csv")
    // 结果日志输出
    printArray(inputCsvFile.collect)

    // 读取 tsv 文件
    val inputTsvFile = spark.read.option("delimiter", "\\t").csv(READ_DATA_DIR + "people.tsv")
    // 结果日志输出
    printArray(inputTsvFile.collect)

    // 停止sc，结束该任务
    spark.stop()
  }
}
