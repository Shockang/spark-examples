package com.shockang.study.spark.core.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}


object CustomPartitioner {

  def main(args: Array[String]): Unit = {
    //1:初始化SparkContext
    val conf = new SparkConf().setAppName("CustomPartitioner").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val list = List("beijing-1", "beijing-2", "beijing-3", "shanghai-1", "shanghai-2", "tianjing-1", "tianjing-2");

    //2:初始数据到RDD
    val rdd: RDD[String] = sc.parallelize(list)
    rdd.cache()
    rdd.persist()

    //3:执行map计算，使用自定义分区，将key中前缀相同的数据分配在同一个分区中计算
    rdd.map((_, 1)).partitionBy(new CustomPartitioner(3)).foreachPartition(t => {
      val id = TaskContext.get.partitionId
      println("partitionNum:" + id)
      t.foreach(data => {
        println(data)
      })
    })
    //4：停止SparkContext
    sc.stop()
  }
}


class CustomPartitioner(numParts: Int) extends Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    //以"-"分割数据，将前缀相同的数据放在一个分区
    val prex = key.toString.split("-").apply(0)
    val code = (prex.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }
}
