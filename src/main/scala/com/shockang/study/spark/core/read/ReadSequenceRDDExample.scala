package com.shockang.study.spark.core.read

import com.shockang.study.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取 SequenceFile
 *
 * @author Shockang
 */
object ReadSequenceRDDExample {
  def main(args: Array[String]): Unit = {

    // 初始化 SparkConf 对象，设置基本任务参数
    val conf: SparkConf = new SparkConf()
      // 设置提交任务的目标 Master 机器地址，local 为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      // 设置任务名称
      .setAppName("ReadSequenceRDDExample")
    // 实例化 SparkContext
    val sc: SparkContext = new SparkContext(conf)
    val path: String = READ_DATA_DIR + "people.sequence"

    // sequenceFile 应当使用 Hadoop 的 Writable 接口实现类，像 IntWritable 和 Text
    // Spark 也允许使用一些原生类型，比如， sequenceFile[Int, String] 会自动转换成 IntWritable 和 Text
    val rdd: RDD[(String, String)] = sc.sequenceFile[String, String](path)
    printArray(rdd.collect)
    sc.stop()
  }
}
