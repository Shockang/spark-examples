package com.shockang.study.spark.core.write


import com.shockang.study.spark._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WriteHadoopRDDExample {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkConf 对象，设置基本任务参数
    val conf: SparkConf = new SparkConf()
      // 设置提交任务的目标 Master 机器地址，local 为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      // 设置任务名称
      .setAppName("WriteHadoopRDDExample")
    // 实例化 SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, String)] = sc.parallelize(Seq(("Michael", "29"), ("Andy", "30"), ("Justin", "19")), 1).cache()

    // 使用 Hadoop 旧 API
    val oldPath: String = HDFS_WRITE_DIR + "WriteHadoopRDDExampleWithOldAPI"
    rdd.saveAsHadoopFile(oldPath, classOf[LongWritable], classOf[Text], classOf[org.apache.hadoop.mapred.TextOutputFormat[LongWritable, Text]])

    // 使用 Hadoop 新 API
    val newPath: String = HDFS_WRITE_DIR + "WriteHadoopRDDExampleWithNewAPI"
    rdd.saveAsNewAPIHadoopFile(newPath, classOf[LongWritable], classOf[Text], classOf[TextOutputFormat[LongWritable, Text]])

    sc.stop
  }
}
