package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Join")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("Alice", 19),
        ("Bob", 20),
        ("Thomas", 30),
        ("Catalina", 25),
        ("Kotlin", 27)),
      2)

    val rddData2 = sc.parallelize(
      Array(
        ("Alice", "female"),
        ("Bob", "male"),
        ("Thomas", "male"),
        ("Catalina", "famale"),
        ("Kotlin", "female")),
      2)

    val rddData3 = rddData1.join(rddData2)

    printArray(rddData3.collect)

    sc.stop()
  }
}
