package com.shockang.study.spark.core.cache

import com.shockang.study.spark.{CHECKPOINT_DIR, printArray}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCheckpointExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkRDDCheckpointExample")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir(CHECKPOINT_DIR)
    sc.setLogLevel("ERROR")

    val rddData1 = sc.parallelize(1 to 10, 2)
    val rddData2 = rddData1.map(_ * 2)
    rddData2.dependencies.head.rdd

    rddData2.persist(StorageLevel.DISK_ONLY)
    rddData2.checkpoint

    rddData2.dependencies.head.rdd
    val rddData3 = rddData2.map(_ + 3)
    val rddData4 = rddData2.map(_ + 4)

    printArray(rddData3.collect())
    printArray(rddData4.collect())

    rddData2.dependencies.head.rdd
    rddData2.unpersist(true)
  }
}
