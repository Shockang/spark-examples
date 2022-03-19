package com.shockang.study.spark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

object Count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Count")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array(("语文", 95), ("数学", 75), ("英语", 88)), 2)
    println(rddData1.count)

    sc.stop()
  }
}
