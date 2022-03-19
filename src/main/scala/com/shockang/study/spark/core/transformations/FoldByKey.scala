package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("FoldByKey")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("会员A", 300f),
        ("会员A", 100f),
        ("会员B", 150f),
        ("会员B", 500f),
        ("会员C", 350f)),
      2)
    val rddData2 = rddData1.foldByKey(-100f)(_ + _)

    printArray(rddData2.collect)

    sc.stop()
  }
}
