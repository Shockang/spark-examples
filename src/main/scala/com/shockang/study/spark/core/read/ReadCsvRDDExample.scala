package com.shockang.study.spark.core.read

import com.shockang.study.spark.READ_DATA_DIR
import org.apache.spark.{SparkConf, SparkContext}

object ReadCsvRDDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ReadCsvRDDExample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //读取CSV文件
    val inputCSVFile = sc.textFile(READ_DATA_DIR + "people.csv").flatMap(_.split(",")).collect
    inputCSVFile.foreach(println)
    //读取TSV文件
    val inputTSVFile = sc.textFile(READ_DATA_DIR + "people.tsv").flatMap(_.split("\t")).collect
    inputTSVFile.foreach(println)

    //停止sc，结束该任务
    sc.stop()
  }
}
