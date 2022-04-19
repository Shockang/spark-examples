package com.shockang.study.spark.sql.codegen

import org.apache.spark.sql.SparkSession

/**
 *
 * @author Shockang
 */
object CodeGenExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CodeGenExample")
      .master("local[*]")
      // .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")

    import spark.implicits._

    val df = Seq(("A", 1, 1), ("B", 2, 1), ("C", 3, 1), ("D", 4, 1), ("E", 5, 1))
      .toDF("letter", "nr", "a_flag")

    df.filter("letter != 'A'")
      .map(row => row.getAs[String]("letter")).count()

    spark.stop()
  }
}
