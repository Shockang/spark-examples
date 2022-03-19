package com.shockang.study.spark.core.write

import com.shockang.study.spark.WRITE_DATA_DIR
import com.shockang.study.spark.util.Utils.writableLocalFsPath
import org.apache.spark.{SparkConf, SparkContext}

object WriteCsvRDDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WriteCsvRDDExample")
    val sc = new SparkContext(conf)

    val array = Array("Thomas", 18, "male", "65kg", "180cm")

    //转换成CSV格式保存
    val csvRDD = sc.parallelize(Array(array.mkString(",")), 1)
    val csvPath = WRITE_DATA_DIR + "WriteCsvRDDExample1"
    csvRDD.saveAsTextFile(writableLocalFsPath(csvPath))

    //转换成TSV格式保存
    val tsvRDD = sc.parallelize(Array(array.mkString("\t")), 1)
    val tsvPath = WRITE_DATA_DIR + "WriteCsvRDDExample2"
    tsvRDD.saveAsTextFile(writableLocalFsPath(tsvPath))

    sc.stop
  }
}
