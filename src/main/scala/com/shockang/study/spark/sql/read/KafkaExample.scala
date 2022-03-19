package com.shockang.study.spark.sql.read

import com.shockang.study.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.nio.file.{Files, Paths}

/**
 *
 * @author Shockang
 */
object KafkaExample {
  def main(args: Array[String]): Unit = {
    // 使用 Spark 2.0 提供的 SparkSession API 来访问应用程序
    val spark = SparkSession.builder().master("local[*]").appName("ParquetExample").getOrCreate()

    import org.apache.spark.sql.avro.functions._

    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get(READ_DATA_DIR + "user.avsc")))

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_URL)
      .option("subscribe", KAFKA_TOPIC)
      .load()

    import spark.implicits._


    val output = df.select(col(from_avro($"value", jsonFormatSchema) as $"user"))
      .where("user.favorite_color == \"red\"")
      .select(col(to_avro($"user.name") as $"value"))

    val query = output
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_URL)
      .option("topic", KAFKA_TOPIC)
      .start()

    query.awaitTermination()

    spark.stop()
  }
}
