package com.shockang.study.spark.sql.write

import com.shockang.study.spark.{MYSQL_DB_TABLE, MYSQL_JDBC_URL, MYSQL_PASS, MYSQL_USER}
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 *
 * @author Shockang
 */
object WriteMysqlDataFrameExample {
  def main(args: Array[String]): Unit = {
    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ReadCsvDataFrameExample").getOrCreate()

    val df = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")

    // Saving data to a JDBC source
    df.write
      .format("jdbc")
      .option("url", MYSQL_JDBC_URL)
      .option("dbtable", MYSQL_DB_TABLE)
      .option("user", MYSQL_USER)
      .option("password", MYSQL_PASS)
      .save()

    val connectionProperties = new Properties()
    connectionProperties.put("user", MYSQL_USER)
    connectionProperties.put("password", MYSQL_PASS)

    df.write
      .jdbc(MYSQL_JDBC_URL, MYSQL_DB_TABLE, connectionProperties)

    df.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc(MYSQL_JDBC_URL, MYSQL_DB_TABLE, connectionProperties)
  }
}
