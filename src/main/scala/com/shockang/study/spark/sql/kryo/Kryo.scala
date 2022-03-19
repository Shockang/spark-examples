package com.shockang.study.spark.sql.kryo

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Row, SparkSession}

/**
 *
 * @author Shockang
 */
object Kryo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Kryo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val schema = StructType(List(
      StructField("movie", StringType, nullable = false),
      StructField("category", StringType, nullable = false)
    ))

    val javaList = new java.util.ArrayList[Row]()
    javaList.add(Row("《疑犯追踪》", "战争,动作,科幻"))
    javaList.add(Row("《叶问》", "动作,战争"))
    val df1 = spark.createDataFrame(javaList, schema)
    df1.show

    implicit val flatMapEncoder: Encoder[(String, String)] = org.apache.spark.sql.Encoders.kryo[(String, String)]
    val tableArray = df1.flatMap(row => {
      val listTuple = new scala.collection.mutable.ListBuffer[(String, String)]()
      val categoryArray = row.getString(1).split(",")
      for (c <- categoryArray) {
        listTuple.append((row.getString(0), c))
      }
      listTuple
    }).collect()
    val df2 = spark.createDataFrame(tableArray).toDF("movie", "category")
    df2.show

  }
}
