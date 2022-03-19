package com.shockang.study.spark.core.actions

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Take {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Take")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array("Thomas", "Alice", "Kotlin"))
    printArray(rddData1.take(2))

    sc.stop()
  }
}
