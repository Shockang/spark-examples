package com.shockang.study.spark.core.demo

import org.apache.spark.sql.SparkSession

import scala.math.random

/**
 * 计算 PI 的近似值
 */
object SparkPI {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkPI")
      .getOrCreate()

    val slices = 1000
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices)
      .map { _ =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
    println("PI is roughly " + 4.0 * count / (n - 1))

    spark.stop()
  }
}
