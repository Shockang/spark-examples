package com.shockang.study.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}


object LongAccumulatorExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("LongAccumulatorExample")
    val sc = new SparkContext(conf)

    val visitorRDD = sc.parallelize(
      Array(
        ("Bob", 15),
        ("Thomas", 28),
        ("Tom", 18),
        ("Galen", 35),
        ("Catalina", 12),
        ("Karen", 9),
        ("Boris", 20)),
      3)

    val visitorAccumulator = sc.longAccumulator("统计成年游客人数")

    visitorRDD.foreach(visitor => {
      if (visitor._2 >= 18) {
        visitorAccumulator.add(1)
      }
    })

    println(visitorAccumulator)
  }
}
