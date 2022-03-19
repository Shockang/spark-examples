package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("FlatMap")
    val sc = new SparkContext(conf)

    val rddData = sc.parallelize(Array("one,two,three", "four,five,six", "seven,eight,nine,ten"))
    val rddData2 = rddData.flatMap(_.split(","))
    printArray(rddData2.collect)

    sc.stop()
  }
}
