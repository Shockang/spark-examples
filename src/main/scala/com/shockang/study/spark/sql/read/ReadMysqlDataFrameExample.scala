package com.shockang.study.spark.sql.read

import com.shockang.study.spark.{MYSQL_DB_TABLE, MYSQL_JDBC_URL, MYSQL_PASS, MYSQL_USER}
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 *
 * @author Shockang
 */
object ReadMysqlDataFrameExample {
  def main(args: Array[String]): Unit = {
    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ReadCsvDataFrameExample").getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", MYSQL_JDBC_URL)
      .option("dbtable", MYSQL_DB_TABLE)
      .option("user", MYSQL_USER)
      .option("password", MYSQL_PASS)
      .load()
    jdbcDF.show()

    val connectionProperties = new Properties()
    connectionProperties.put("user", MYSQL_USER)
    connectionProperties.put("password", MYSQL_PASS)
    val jdbcDF2 = spark.read
      .jdbc(MYSQL_JDBC_URL, MYSQL_DB_TABLE, connectionProperties)
    jdbcDF2.show()

    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    val jdbcDF3 = spark.read
      .jdbc(MYSQL_JDBC_URL, MYSQL_DB_TABLE, connectionProperties)
    jdbcDF3.show()
  }
}
