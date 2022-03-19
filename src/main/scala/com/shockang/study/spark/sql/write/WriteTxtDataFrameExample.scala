package com.shockang.study.spark.sql.write

import com.shockang.study.spark.WRITE_DATA_DIR
import com.shockang.study.spark.util.Utils.writableLocalFsPath
import org.apache.spark.sql.SparkSession

/**
 * 写入数据到 txt 文件
 *
 * @author Shockang
 */
object WriteTxtDataFrameExample {

  case class Person(name: String, sex: String, age: Int)

  def main(args: Array[String]): Unit = {

    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("UsingSqlWriteTxtExample").getOrCreate()

    // 注意隐式导入
    import spark.implicits._

    // 由于 txt 只支持 String 类型的数据，故需要经过转换
    val df = spark.createDataFrame(List(("one", 1), ("two", 2), ("three", 3))).map(_.mkString("(", ", ", ")"))

    val filePath = WRITE_DATA_DIR + "WriteTxtDataFrameExample"

    df.write.text(writableLocalFsPath(filePath))

    spark.stop
  }
}
