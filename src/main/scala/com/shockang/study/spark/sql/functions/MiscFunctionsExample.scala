package com.shockang.study.spark.sql.functions

import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 *
 * @author Shockang
 */
object MiscFunctionsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MiscFunctionsExample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = Seq(("ABC", Array[Byte](1, 2, 3, 4, 5, 6))).toDF("a", "b")

    // md5
    formatPrint("""df.select(md5($"a"), md5($"b")).show()""")
    df.select(md5($"a"), md5($"b")).show()

    // sha1
    formatPrint("""df.select(sha1($"a"), sha1($"b")).show()""")
    df.select(sha1($"a"), sha1($"b")).show()

    // sha2
    formatPrint("""df.select(sha2($"a", 256), sha2($"b", 256)).show()""")
    df.select(sha2($"a", 256), sha2($"b", 256)).show()

    // crc32
    formatPrint("""df.select(crc32($"a"), crc32($"b")).show()""")
    df.select(crc32($"a"), crc32($"b")).show()

    // hash
    formatPrint("""df.select(hash($"a"), hash($"b")).show()""")
    df.select(hash($"a"), hash($"b")).show()

    // xxhash64
    formatPrint("""df.select(xxhash64($"a"), xxhash64($"b")).show()""")
    df.select(xxhash64($"a"), xxhash64($"b")).show()

    // assert_true
    formatPrint("""Seq((0, 1)).toDF("a", "b").select(assert_true($"a" < $"b")).show()""")
    Seq((0, 1)).toDF("a", "b").select(assert_true($"a" < $"b")).show()

    // 最后两个都会抛出异常中断执行

    formatPrint("""Seq((1, 0)).toDF("a", "b").select(assert_true($"a" < $"b", lit("a >= b"))).show()""")
    Seq((1, 0)).toDF("a", "b").select(assert_true($"a" < $"b", lit("a >= b"))).show()

    // raise_error
    formatPrint("""df.select(raise_error(lit(null.asInstanceOf[String]))).show()""")
    df.select(raise_error(lit(null.asInstanceOf[String]))).show()

    spark.stop()
  }
}
