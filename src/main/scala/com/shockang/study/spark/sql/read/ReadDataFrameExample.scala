package com.shockang.study.spark.sql.read

import com.shockang.study.spark._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 *
 * @author Shockang
 */
object ReadDataFrameExample {
  case class User(name: String, age: Int, sex: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ReadDataFrameExample")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    val df3_1 = spark.read.json(READ_DATA_DIR + "")
    df3_1.show
    val df3_2 = spark.read.csv(READ_DATA_DIR + "").toDF("name", "age", "sex")
    df3_2.show
    val df3_2_1 = spark.read.csv(READ_DATA_DIR + "")
    df3_2_1.show

    import spark.implicits._

    val ds3_1 = df3_1.as[User]
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    ds3_1.write.parquet(READ_DATA_DIR + "")
    val df4 = spark.read.load(READ_DATA_DIR + "")
    df4.show

    val df5_1 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    df5_1.show()


    val schema = StructType(List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("sex", StringType, true)
    ))

    val javaList = new java.util.ArrayList[Row]()
    javaList.add(Row("Alice", 20, "Female"))
    javaList.add(Row("Tom", 18, "Male"))
    javaList.add(Row("Boris", 30, "Male"))
    val df5_2 = spark.createDataFrame(javaList, schema)
    df5_2.show

    val options = Map("url" -> MYSQL_JDBC_URL,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> MYSQL_USER,
      "password" -> MYSQL_PASS,
      "dbtable" -> "CDS")
    val df6 = spark.read.format("jdbc").options(options).load()
    df6.show

    val df7_1 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")

    val properties = new java.util.Properties()
    properties.setProperty("user", MYSQL_USER)
    properties.setProperty("password", MYSQL_PASS)

    import org.apache.spark.sql.SaveMode
    df7_1.write.mode(SaveMode.Append).jdbc(MYSQL_JDBC_URL, "CDS", properties)

    val df7_2 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    df7_2.repartition(1).write.format("parquet").save(WRITE_DATA_DIR + "")

    val df7_3 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    df7_3.repartition(1).write.json(WRITE_DATA_DIR + "")
    df7_3.repartition(1).write.csv(WRITE_DATA_DIR + "")
  }
}
