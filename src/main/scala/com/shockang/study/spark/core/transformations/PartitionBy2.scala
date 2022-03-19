package com.shockang.study.spark.core.transformations

import com.shockang.study.spark.printArray
import org.apache.spark.{SparkConf, SparkContext}

object PartitionBy2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("PartitionBy2")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    import org.apache.spark.HashPartitioner

    val rddData1 = sc.parallelize(Array(("Alice", 15), ("Bob", 18), ("Thomas", 20), ("Catalina", 25))).cache()
    val rddData2 = sc.parallelize(Array(("Alice", "Female"), ("Thomas", "Male"), ("Tom", "Male"))).cache()

    println(rddData1.partitions.length)
    println(rddData2.partitions.length)

    val rddData3 = rddData1.partitionBy(new HashPartitioner(2))
    val rddData4 = rddData2.partitionBy(new HashPartitioner(2))

    val rddData5 = rddData3.join(rddData4, 2)
    printArray(rddData5.collect)

    val rddData6 = rddData1.partitionBy(new HashPartitioner(3))
    val rddData7 = rddData2.partitionBy(new HashPartitioner(3))

    val rddData8 = rddData6.join(rddData7, 2)
    printArray(rddData8.collect)

    val rddData9 = rddData1.partitionBy(new HashPartitioner(3))
    val rddData10 = rddData2.partitionBy(new HashPartitioner(2))
    val rddData11 = rddData9.join(rddData10, 2)

    printArray(rddData11.collect)

  }
}
