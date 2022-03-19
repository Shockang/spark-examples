package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object RandomSplit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RandomSplit")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rddData1 = sc.parallelize(1 to 10, 3)
    val splitRDD = rddData1.randomSplit(Array(1, 4, 5))

    printArray(splitRDD(0).collect)
    printArray(splitRDD(1).collect)
    printArray(splitRDD(2).collect)

    sc.stop()
  }
}
