package com.shockang.study.spark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

object First {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("First")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array("Thomas", "Alice", "Kotlin"))
    println(rddData1.first)

    sc.stop()
  }
}
