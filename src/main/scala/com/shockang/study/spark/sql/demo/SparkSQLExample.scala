package com.shockang.study.spark.sql.demo

import com.shockang.study.spark.SQL_DATA_DIR
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @author Shockang
 */
object SparkSQLExample {

  val DATA_PATH = SQL_DATA_DIR + "user.json"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local[*]").appName("SparkSQLExample").getOrCreate()

    val df = spark.read.json(DATA_PATH)
    df.createTempView("t_user")

    spark.sql("SELECT * FROM t_user").show

    spark.stop()
  }
}
