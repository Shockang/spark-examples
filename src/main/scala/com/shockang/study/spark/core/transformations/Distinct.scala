package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Distinct")
    val sc = new SparkContext(conf)

    val rddData = sc.parallelize(Array("Alice", "Nick", "Alice", "Kotlin", "Catalina", "Catalina"), 3)
    val rddData2 = rddData.distinct
    printArray(rddData2.collect)

    sc.stop()
  }
}
