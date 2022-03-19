package com.shockang.study.spark.core.cache

import com.shockang.study.spark.SOGOU_DATA_PATH
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCacheExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkRDDCacheExample")
    val sc = new SparkContext(conf)
    // 设置一下 Spark 日志打印的级别，方便查看控制台打印
    sc.setLogLevel("ERROR")

    val sogouRDD = sc.textFile(SOGOU_DATA_PATH, 8).cache()

    val rddData1 = sogouRDD.filter(t => t.split("\t")(0) >= "12:00:00")
    val rddData2 = sogouRDD.map(t => (t.split("\t")(1), 1))

    val cacheStart = System.currentTimeMillis()
    println(rddData1.count)
    println(rddData2.count)
    println(rddData1.count)
    println(rddData2.count)
    printf("cache time:%d", System.currentTimeMillis() - cacheStart)
    println()

    sogouRDD.unpersist(true)

    val noCacheStart = System.currentTimeMillis()
    println(rddData1.count)
    println(rddData2.count)
    println(rddData1.count)
    println(rddData2.count)
    printf("no cache time:%d", System.currentTimeMillis() - noCacheStart)
  }
}
