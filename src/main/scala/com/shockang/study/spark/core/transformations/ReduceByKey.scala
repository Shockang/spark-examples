package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ReduceByKey")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("Alice", 95),
        ("Bob", 37),
        ("Thomas", 100),
        ("Catalina", 77),
        ("Karen", 87)), 2)
    val rddData2 = rddData1.map({
      case (_, grade) if grade <= 60 => ("C", 1)
      case (_, grade) if grade > 60 && grade < 80 => ("B", 1)
      case (_, grade) if grade >= 80 => ("A", 1)
    })
    val rddData3 = rddData2.reduceByKey(_ + _)

    printArray(rddData3.collect)
    sc.stop()
  }
}
