package com.shockang.study.spark.core.read

import org.apache.spark.{SparkConf, SparkContext}

object ReadCollectionRDDExample {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkConf 对象，设置基本任务参数
    val conf: SparkConf = new SparkConf()
      // 设置提交任务的目标 Master 机器地址，local 为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      // 设置任务名称
      .setAppName("ReadObjectRDDExample")

    //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    val sc = new SparkContext(conf)

    val numbers = 1 to 100
    val rdd = sc.parallelize(numbers)

    //1+2=3 3+3= 6 6+4=10 .....
    val sum = rdd.reduce(_ + _)

    println("1+2+....+ 99+100 = " + sum)
  }

}
