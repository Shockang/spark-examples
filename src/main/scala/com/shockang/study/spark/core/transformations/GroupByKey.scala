package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GroupByKey")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("班级1", "Alice"),
        ("班级2", "Tom"),
        ("班级1", "Catalina"),
        ("班级3", "Murdoch"),
        ("班级2", "Fisker")),
      2)

    val rddData2 = rddData1.groupByKey()

    printArray(rddData2.collect)

    sc.stop()
  }
}
