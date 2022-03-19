package com.shockang.study.spark.core.write

import com.shockang.study.spark._
import com.shockang.study.spark.util.Utils.writableLocalFsPath
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 写入数据到 txt 文件
 *
 * @author Shockang
 */
object WriteTxtRDDExample {
  def main(args: Array[String]): Unit = {

    // 初始化 SparkConf 对象，设置基本任务参数
    val conf: SparkConf = new SparkConf()
      // 设置提交任务的目标 Master 机器地址，local 为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      // 设置任务名称
      .setAppName("WriteTxtRDDExample")
    // 实例化 SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("one", 1), ("two", 2), ("three", 3)), 1)
    val filePath: String = WRITE_DATA_DIR + "WriteTxtRDDExample"
    rdd.saveAsTextFile(writableLocalFsPath(filePath))

    sc.stop
  }
}
