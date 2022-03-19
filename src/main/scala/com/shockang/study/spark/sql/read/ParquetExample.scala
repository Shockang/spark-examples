package com.shockang.study.spark.sql.read

import com.shockang.study.spark._
import org.apache.spark.sql.SparkSession

/**
 *
 * @author Shockang
 */
object ParquetExample {
  def main(args: Array[String]): Unit = {
    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ParquetExample").getOrCreate()

    import spark.implicits._

    val peopleDF = spark.read.json(READ_DATA_DIR + "people.json")

    peopleDF.write.parquet(WRITE_DATA_DIR + "people.parquet")

    val parquetFileDF = spark.read.parquet(WRITE_DATA_DIR + "people.parquet")

    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 10 AND 20")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
  }
}
