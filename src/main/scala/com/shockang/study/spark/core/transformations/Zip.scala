package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Zip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Zip")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 3, 2)
    val rddData2 = sc.parallelize(Array("A", "B", "C"), 2)
    val rddData3 = rddData1.zip(rddData2)

    printArray(rddData3.collect)

    sc.stop()
  }
}
