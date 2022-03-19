package com.shockang.study.spark.core.read

import com.shockang.study.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取 object 文件
 *
 * @author Shockang
 */
object ReadObjectRDDExample {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    // 初始化 SparkConf 对象，设置基本任务参数
    val conf: SparkConf = new SparkConf()
      // 设置提交任务的目标 Master 机器地址，local 为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      // 设置任务名称
      .setAppName("ReadObjectRDDExample")

    // 实例化 SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val filePath: String = READ_DATA_DIR + "people.object"
    val rdd: RDD[Person] = sc.objectFile[Person](filePath)

    printArray(rdd.collect)

    sc.stop()
  }
}
