package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Map")
    val sc = new SparkContext(conf)

    val rddData = sc.parallelize(1 to 10)
    val rddData2 = rddData.map(_ * 10)
    printArray(rddData2.collect)

    sc.stop()
  }
}
