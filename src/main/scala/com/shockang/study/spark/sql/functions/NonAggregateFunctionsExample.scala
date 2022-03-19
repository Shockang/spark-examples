package com.shockang.study.spark.sql.functions

import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 *
 * @author Shockang
 */
object NonAggregateFunctionsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NonAggregateFunctionsExample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = Seq(0.0d, -0.0d, 0.0d / 0.0d, Double.NaN).toDF("d")

    // Column
    formatPrint("""df.select(col("d")).show()""")
    df.select(col("d")).show()

    formatPrint("""df.select(column("d")).show()""")
    df.select(column("d")).show()

    formatPrint("""df.select(lit(1)).show()""")
    df.select(lit(1)).show()

    formatPrint("""df.select(typedLit(List(1, 2, 3))).show()""")
    df.select(typedLit(List(1, 2, 3))).show()

    formatPrint("""df.select(typedlit(Map(1 -> 1, 2 -> 2, 3 -> 3))).show()""")
    df.select(typedlit(Map(1 -> 1, 2 -> 2, 3 -> 3))).show()

    // array
    formatPrint("""df.select(array($"d")).show()""")
    df.select(array($"d")).show()

    formatPrint("""df.select(array("d")).show()""")
    df.select(array("d")).show()

    // map/map_from_arrays
    formatPrint("""df.as[Double].map(1 -> _).toDF("a", "b").select(map($"a" + 1, $"b")).show()""")
    df.as[Double].map(1 -> _).toDF("a", "b").select(map($"a" + 1, $"b")).show()

    formatPrint("""df.as[Double].map(Array(_)).toDF("d").select(map_from_arrays($"d", typedlit(Array(1)))).show()""")
    df.as[Double].map(Array(_)).toDF("d").select(map_from_arrays($"d", typedlit(Array(1)))).show()

    // broadcast
    formatPrint("""df.as[Double].map((_, 1)).join(broadcast(df.as[Double].map((_, 2)))).show()""")
    df.as[Double].map((_, 1)).join(broadcast(df.as[Double].map((_, 2)))).show()

    // coalesce
    formatPrint("""df.select(coalesce($"d")).show()""")
    df.select(coalesce($"d")).show()

    // input_file_name
    formatPrint("""df.select(input_file_name()).show()""")
    df.select(input_file_name()).show()

    // isnan/isnull
    formatPrint("""df.select(isnan($"d")).show()""")
    df.select(isnan($"d")).show()

    formatPrint("""df.select(isnull($"d")).show()""")
    df.select(isnull($"d")).show()

    // monotonically_increasing_id
    formatPrint("""df.select(monotonicallyIncreasingId()).show()""")
    df.select(monotonicallyIncreasingId()).show()

    formatPrint("""df.select(monotonically_increasing_id()).show()""")
    df.select(monotonically_increasing_id()).show()

    // nanvl
    formatPrint("""df.select(nanvl($"d", lit(1))).show()""")
    df.select(nanvl($"d", lit(1))).show()

    // negate
    formatPrint("""df.select(-df("d")).show()""")
    df.select(-df("d")).show()

    formatPrint("""df.select(negate(df("d"))).show()""")
    df.select(negate(df("d"))).show()

    // not
    formatPrint("""df.select(not(isnan($"d"))).show()""")
    df.select(not(isnan($"d"))).show()

    // rand/randn
    formatPrint("""df.select(rand(1L)).show()""")
    df.select(rand(1L)).show()

    formatPrint("""df.select(rand()).show()""")
    df.select(rand()).show()

    formatPrint("""df.select(randn(1L)).show()""")
    df.select(randn(1L)).show()

    formatPrint("""df.select(randn()).show()""")
    df.select(randn()).show()

    // spark_partition_id
    formatPrint("""df.select(spark_partition_id()).show()""")
    df.select(spark_partition_id()).show()

    // sqrt
    formatPrint("""df.select(sqrt($"d")).show()""")
    df.select(sqrt($"d")).show()

    formatPrint("""df.select(sqrt("d")).show()""")
    df.select(sqrt("d")).show()

    // struct
    formatPrint("""df.select(struct($"d")).show()""")
    df.select(struct($"d")).show()

    formatPrint("""df.select(struct("d")).show()""")
    df.select(struct("d")).show()

    // when
    formatPrint("""df.select(when(isnan($"d"), 0).otherwise(1)).show()""")
    df.select(when(isnan($"d"), 0).otherwise(1)).show()

    // bitwise_not
    formatPrint("""df.select(bitwiseNOT(lit(1))).show()""")
    df.select(bitwiseNOT(lit(1))).show()

    formatPrint("""df.select(bitwise_not(lit(1))).show()""")
    df.select(bitwise_not(lit(1))).show()

    // expr
    formatPrint("""df.select(expr("d + 1")).show()""")
    df.select(expr("d + 1")).show()

    // greatest
    formatPrint("""df.select(greatest($"d", lit(1))).show()""")
    df.select(greatest($"d", lit(1))).show()

    formatPrint("""df.as[Double].map((_, 1)).toDF("a", "b").select(greatest("a", "b")).show()""")
    df.as[Double].map((_, 1)).toDF("a", "b").select(greatest("a", "b")).show()

    // least
    formatPrint("""df.select(least($"d", lit(1))).show()""")
    df.select(least($"d", lit(1))).show()

    formatPrint("""df.as[Double].map((_, 1)).toDF("a", "b").select(least("a", "b")).show()""")
    df.as[Double].map((_, 1)).toDF("a", "b").select(least("a", "b")).show()

    spark.stop()
  }
}
