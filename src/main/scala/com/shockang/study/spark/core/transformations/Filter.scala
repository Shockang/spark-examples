package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Filter")
    val sc = new SparkContext(conf)

    val rddData = sc.parallelize(1 to 100)
    import scala.util.control.Breaks._
    val rddData2 = rddData.filter(n => {
      // TODO 过滤出质数，此处可用 埃氏筛选法 优化
      var flag = if (n < 2) false else true
      breakable {
        for (x <- 2 until n) {
          if (n % x == 0) {
            flag = false
            break
          }
        }
      }
      flag
    })

    printArray(rddData2.collect)

    sc.stop()
  }
}
