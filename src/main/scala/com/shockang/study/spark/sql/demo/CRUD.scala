package com.shockang.study.spark.sql.demo

import com.shockang.study.spark.{CHECKPOINT_DIR, READ_DATA_DIR}
import org.apache.spark.sql.SparkSession

/**
 *
 * @author Shockang
 */
object CRUD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CRUD")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    sc.setCheckpointDir(CHECKPOINT_DIR)

    spark.sql(
      """
         CREATE TABLE IF NOT EXISTS t_fruit(
         name string,
         color string,
         count int)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY ","
         STORED AS TEXTFILE
      """).checkpoint()

    spark.sql(
      """
        DELETE FROM t_fruit
      """)

    spark.sql(
      s"""
        LOAD DATA LOCAL INPATH "${READ_DATA_DIR}/t_fruit.csv"
        INTO TABLE t_fruit
      """)

    spark.sql(
      """
         SELECT * FROM t_fruit
      """).show


  }
}
