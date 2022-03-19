package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object MapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapValues")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("Alice", 3500),
        ("Bob", 2000),
        ("Thomas", 10000),
        ("Catalina", 2000),
        ("Kotlin", 4000),
        ("Karen", 9000)), 2)
    val rddData2 = rddData1.mapValues(_ + "元")

    printArray(rddData2.collect)

    sc.stop()
  }
}
