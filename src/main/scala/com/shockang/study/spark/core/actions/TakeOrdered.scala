package com.shockang.study.spark.core.actions

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object TakeOrdered {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TakeOrdered")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array(("Alice", 95), ("Tom", 75), ("Thomas", 88)), 2)
    printArray(rddData1.takeOrdered(2)(Ordering.by(t => t._2)))

    sc.stop()
  }
}
