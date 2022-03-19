package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Intersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Intersection")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array(1, 1, 2))
    val rddData2 = sc.parallelize(Array(2, 2, 3))
    val rddData3 = rddData1.intersection(rddData2)

    printArray(rddData3.collect)
    sc.stop()
  }
}
