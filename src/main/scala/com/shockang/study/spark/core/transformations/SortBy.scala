package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object SortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SortBy")
    val sc = new SparkContext(conf)


    val rddData1 = sc.parallelize(Array(("dog", 3), ("cat", 1), ("hadoop", 2), ("spark", 3), ("apple", 2)))
    val rddData2 = rddData1.sortBy(_._2, ascending = false)
    printArray(rddData2.collect)

    sc.stop()
  }
}
