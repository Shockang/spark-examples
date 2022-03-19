package com.shockang.study.spark.sql.write

import com.shockang.study.spark.WRITE_DATA_DIR
import com.shockang.study.spark.util.Utils.writableLocalFsPath
import org.apache.spark.sql.SparkSession

/**
 * 写入数据到 csv/tsv 文件
 *
 * @author Shockang
 */
object WriteCsvDataFrameExample {
  def main(args: Array[String]): Unit = {

    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("UsingSqlWriteTxtExample").getOrCreate()

    //转换成CSV格式保存
    val df = spark.createDataFrame(List(("one", 1), ("two", 2), ("three", 3)))
    val csvPath = WRITE_DATA_DIR + "WriteCsvDataFrameExample1"
    df.write.csv(writableLocalFsPath(csvPath))

    //转换成TSV格式保存
    val tsvPath = WRITE_DATA_DIR + "WriteCsvDataFrameExample2"
    df.write.option("delimiter", "\\t").csv(writableLocalFsPath(tsvPath))

    spark.stop
  }
}
