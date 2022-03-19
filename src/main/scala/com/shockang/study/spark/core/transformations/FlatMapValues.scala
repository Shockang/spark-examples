package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("FlatMapValues")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("文件A", "cat\tdog\thadoop\tcat"),
        ("文件A", "cat\tdog\thadoop\tspark"),
        ("文件B", "cat\tspark\thadoop\tcat"),
        ("文件B", "spark\tdog\tspark\tcat")),
      2)

    val rddData2 = rddData1.flatMapValues(_.split("\t"))

    val rddData3 = rddData2.map((_, 1))

    val rddData4 = rddData3.reduceByKey(_ + _).sortBy(_._1._1)

    printArray(rddData4.collect)

    sc.stop()
  }
}
