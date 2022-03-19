package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Union {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Union")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 10)
    val rddData2 = sc.parallelize(1 to 20)
    val rddData3 = rddData1.union(rddData2)

    printArray(rddData3.collect)
    sc.stop()
  }
}
