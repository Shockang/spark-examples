package com.shockang.study.spark.core.actions

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Collect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Collect")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 5)
    printArray(rddData1.collect)

    sc.stop()
  }
}
