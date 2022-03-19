package com.shockang.study.spark.core.read

import com.shockang.study.spark._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadHadoopRDDExample {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkConf 对象，设置基本任务参数
    val conf: SparkConf = new SparkConf()
      // 设置提交任务的目标 Master 机器地址，local 为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      // 设置任务名称
      .setAppName("ReadHadoopRDDExample")
    // 实例化 SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val path: String = HDFS_READ_DIR + "people.txt"

    // 使用 Hadoop 旧 API
    val oldHadoopRDD: RDD[(LongWritable, Text)] = sc.hadoopFile[LongWritable, Text, mapred.TextInputFormat](path)
    printArray(oldHadoopRDD.map(_._2.toString).collect)

    // 使用 Hadoop 新 API
    val newHadoopRDD: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](path)
    printArray(newHadoopRDD.map(_._2.toString).collect)

    sc.stop
  }
}
