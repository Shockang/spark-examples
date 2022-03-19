package com.shockang.study.spark.core.read

import com.shockang.study.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取 txt 文件
 *
 * @author Shockang
 */
object ReadTxtRDDExample {
  def main(args: Array[String]): Unit = {

    // 初始化 SparkConf 对象，设置基本任务参数
    val conf: SparkConf = new SparkConf()
      // 设置提交任务的目标 Master 机器地址，local 为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      // 设置任务名称
      .setAppName("ReadTxtRDDExample")
    // 实例化 SparkContext
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // 读取文本文件，支持目录，压缩文件和通配符
    val txtRDD: RDD[String] = sc.textFile(READ_DATA_DIR + "people.txt")
    printArray(txtRDD.collect)

    val dirRDD: RDD[String] = sc.textFile(READ_DATA_DIR)
    printArray(dirRDD.collect)

    // 默认支持 gzip 压缩
    val gzipRDD: RDD[String] = sc.textFile(READ_DATA_DIR + "people.gz")
    printArray(gzipRDD.collect)

    val wildcardsRDD: RDD[String] = sc.textFile(READ_DATA_DIR + "*.txt")
    printArray(wildcardsRDD.collect)

    // 支持自定义分区数
    val txtRDDWithMinPartitions: RDD[String] = sc.textFile(READ_DATA_DIR + "people.txt", 1)
    printArray(txtRDDWithMinPartitions.collect)

    // 读取目录下所有文本文件
    val wholeTxtRDD: RDD[(String, String)] = sc.wholeTextFiles(READ_DATA_DIR)
    // 结果日志输出，返回（文件名称，文件内容）对
    printArray(wholeTxtRDD.collect)

    // 读取目录下所有文本文件，同样支持自定义分区数
    val wholeTxtRDDWithMinPartitions: RDD[(String, String)] = sc.wholeTextFiles(READ_DATA_DIR, 1)
    // 结果日志输出，返回（文件名称，文件内容）对
    printArray(wholeTxtRDDWithMinPartitions.collect)

    // 停止sc，结束该任务
    sc.stop()
  }
}
