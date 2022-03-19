package com.shockang.study.spark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

object Foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Foreach")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("用户1", "接口1"),
        ("用户2", "接口1"),
        ("用户1", "接口1"),
        ("用户1", "接口2"),
        ("用户2", "接口3")),
      2)
    rddData1.foreach(println)

    sc.stop()
  }
}
