package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object ZipWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ZipWithIndex")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array("A", "B", "C", "D", "E"), 2)
    val rddData2 = rddData1.zipWithIndex()
    printArray(rddData2.collect)

    sc.stop()
  }
}
