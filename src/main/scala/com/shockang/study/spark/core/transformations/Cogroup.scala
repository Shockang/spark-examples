package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Cogroup")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("Alice", 19),
        ("Bob", 20),
        ("Thomas", 30),
        ("Catalina", 25),
        ("Kotlin", 27),
        ("Karen", 99)),
      2)

    val rddData2 = sc.parallelize(
      Array(
        ("Alice", "female"),
        ("Bob", "male"),
        ("Thomas", "male"),
        ("Catalina", "famale"),
        ("Kotlin", "female")),
      2)

    val rddData3 = sc.parallelize(
      Array(
        ("Alice", "Address1"),
        ("Alice", "Address2"),
        ("Bob", "Address3"),
        ("Thomas", "Address4"),
        ("Catalina", "Address5"),
        ("Kotlin", "Address6")),
      2)

    val rddData4 = rddData1.cogroup(rddData2, rddData3)

    printArray(rddData4.collect)
    sc.stop()
  }
}
