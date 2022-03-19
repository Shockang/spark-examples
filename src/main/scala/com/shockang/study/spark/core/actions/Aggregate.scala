package com.shockang.study.spark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Aggregate")
    val sc = new SparkContext(conf)

    import collection.mutable.ListBuffer
    val rddData1 = sc.parallelize(
      Array(
        ("用户1", "接口1"),
        ("用户2", "接口1"),
        ("用户1", "接口1"),
        ("用户1", "接口2"),
        ("用户2", "接口3")),
      2)

    val result = rddData1.aggregate(ListBuffer[(String)]())(
      (list: ListBuffer[String], tuple: (String, String)) => list += tuple._2,
      (list1: ListBuffer[String], list2: ListBuffer[String]) => list1 ++= list2)

    println(result)
    sc.stop()
  }
}
