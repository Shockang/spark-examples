package com.shockang.study.spark.sql.hive

import com.shockang.study.spark.HIVE_URL
import org.apache.spark.sql.SparkSession

/**
 *
 * @author Shockang
 */
object SQLOnHive {
  def main(args: Array[String]): Unit = {
    //1:初始化SparkContext
    val spark = SparkSession
      .builder()
      .master(HIVE_URL)
      .master("local")
      .appName("SQLOnHive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show tables").show()
    spark.sql(" CREATE TABLE persion(name STRING,age INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n';")

    spark.stop()
  }
}
