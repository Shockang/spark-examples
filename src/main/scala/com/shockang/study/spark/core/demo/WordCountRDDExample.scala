package com.shockang.study.spark.core.demo

import com.shockang.study.spark.util.Utils.writableLocalFsPath
import com.shockang.study.spark.{READ_DATA_DIR, WRITE_DATA_DIR}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author Shockang
 */
object WordCountRDDExample {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf对象，设置基本任务参数
    val conf = new SparkConf()
      //设置目标Master机器地址
      .setMaster("local[*]")
      //设置任务名称
      .setAppName("WordCountRDDExample")

    //实例化SparkContext，Spark的对外接口，即负责用户与Spark内部的交互通信
    val sc = new SparkContext(conf)

    //读取文件并进行单词统计
    sc.textFile(READ_DATA_DIR + "words.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 1)
      .sortBy(_._2, ascending = false)
      .saveAsTextFile(writableLocalFsPath(WRITE_DATA_DIR + "WordCountRDDExample"))

    //停止sc，结束该任务
    sc.stop()
  }
}
