package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SortByKey")
    val sc = new SparkContext(conf)


    val rddData1 = sc.parallelize(Array(("dog", 3), ("cat", 1), ("hadoop", 2), ("spark", 3), ("apple", 2)))
    val rddData2 = rddData1.sortByKey()
    printArray(rddData2.collect)

    sc.stop()
  }
}
