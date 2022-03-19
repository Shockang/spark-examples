package com.shockang.study.spark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

object AggregateActionsExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("AggregateActionsExample")
    val sc = new SparkContext(conf)
    // 设置一下 Spark 日志打印的级别，方便查看控制台打印
    sc.setLogLevel("ERROR")

    val numRDD = sc.parallelize(1 to 10)
    println(numRDD.sum)
    println(numRDD.max)
    println(numRDD.min)
    println(numRDD.mean)
    println(numRDD.variance)
    println(numRDD.sampleVariance)
    println(numRDD.stdev)
    println(numRDD.sampleStdev)

    sc.stop()
  }
}
