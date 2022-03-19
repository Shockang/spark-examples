package com.shockang.study.spark.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Coalesce")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rddData1 = sc.parallelize(1 to 100, 10)
    println(rddData1.partitions.length)

    val rddData2 = rddData1.coalesce(5)
    println(rddData2.partitions.length)

    val rddData3 = rddData2.coalesce(7)
    println(rddData3.partitions.length)

    sc.stop()
  }
}
