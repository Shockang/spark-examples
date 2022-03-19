package com.shockang.study.spark.sql.functions

import com.shockang.study.spark.READ_DATA_DIR
import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Aggregate functions
 *
 * @author Shockang
 */
object AggregateFunctionsExample {
  val DATA_PATH = READ_DATA_DIR + "employees.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AggregateFunctionsExample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.csv(DATA_PATH).toDF("name", "dept", "age", "salary").cache()

    // approx_count_distinct
    formatPrint("""df.select(approxCountDistinct(col("age")))""")
    df.select(approxCountDistinct(col("age"))).show()

    formatPrint("""df.select(approxCountDistinct("age"))""")
    df.select(approxCountDistinct("age")).show()

    formatPrint("""df.select(approxCountDistinct(col("age"), 0.05))""")
    df.select(approxCountDistinct(col("age"), 0.05)).show()

    formatPrint("""df.select(approxCountDistinct("age", 0.05))""")
    df.select(approxCountDistinct("age", 0.05)).show()

    formatPrint("""df.select(approx_count_distinct(col("age")))""")
    df.select(approx_count_distinct(col("age"))).show()

    formatPrint("""df.select(approx_count_distinct("age"))""")
    df.select(approx_count_distinct("age")).show()

    formatPrint("""df.select(approx_count_distinct(col("age"), 0.05))""")
    df.select(approx_count_distinct(col("age"), 0.05)).show()

    formatPrint("""df.select(approx_count_distinct("age", 0.05))""")
    df.select(approx_count_distinct("age", 0.05)).show()

    // avg
    formatPrint("""df.select(avg(col("age")))""")
    df.select(avg(col("age"))).show()
    formatPrint("""df.select(avg("age"))""")
    df.select(avg("age")).show()

    // collect_list/collect_set
    formatPrint("""df.select(collect_list(col("age")))""")
    df.select(collect_list(col("age"))).show()

    formatPrint("""df.select(collect_list("age"))""")
    df.select(collect_list("age")).show()

    // collect_list/collect_set
    formatPrint("""df.select(collect_set(col("age")))""")
    df.select(collect_set(col("age"))).show()

    formatPrint("""df.select(collect_set("age"))""")
    df.select(collect_set("age")).show()

    // corr
    formatPrint("""df.select(corr(col("age"), col("salary")))""")
    df.select(corr(col("age"), col("salary"))).show()

    formatPrint("""df.select(corr("age", "salary"))""")
    df.select(corr("age", "salary")).show()

    // count/count_distinct
    formatPrint("""df.select(count(col("age")))""")
    df.select(count(col("age"))).show()

    formatPrint("""df.select(count("age"))""")
    df.select(count("age")).show()

    formatPrint("""df.select(countDistinct(col("age")))""")
    df.select(countDistinct(col("age"))).show()

    formatPrint("""df.select(countDistinct("age"))""")
    df.select(countDistinct("age")).show()

    formatPrint("""df.select(count_distinct(col("age")))""")
    df.select(count_distinct(col("age"))).show()

    formatPrint("""df.select(count_distinct(col("age"), col("salary")))""")
    df.select(count_distinct(col("age"), col("salary"))).show()

    // covar_pop/covar_samp
    formatPrint("""df.select(covar_pop(col("age"), col("salary")))""")
    df.select(covar_pop(col("age"), col("salary"))).show()

    formatPrint("""df.select(covar_pop("age", "salary"))""")
    df.select(covar_pop("age", "salary")).show()

    formatPrint("""df.select(covar_samp(col("age"), col("salary")))""")
    df.select(covar_samp(col("age"), col("salary"))).show()

    formatPrint("""df.select(covar_samp("age", "salary"))""")
    df.select(covar_samp("age", "salary")).show()

    // first
    formatPrint("""df.select(first(col("age"), ignoreNulls = true))""")
    df.select(first(col("age"), ignoreNulls = true)).show()

    formatPrint("""df.select(first("age", ignoreNulls = true))""")
    df.select(first("age", ignoreNulls = true)).show()

    formatPrint("""df.select(first(col("age")))""")
    df.select(first(col("age"))).show()

    formatPrint("""df.select(first("age"))""")
    df.select(first("age")).show()

    // grouping/grouping_id
    formatPrint("""df.cube("age","salary").agg(grouping(col("age")),grouping(col("salary")))""")
    df.cube("age", "salary").agg(grouping(col("age")), grouping(col("salary")), grouping_id(col("age"), col("salary"))).show()

    formatPrint("""df.cube("age","salary").agg(grouping("age"),grouping("salary"))""")
    df.cube("age", "salary").agg(grouping("age"), grouping("salary"), grouping_id("age", "salary")).show()

    // kurtosis
    formatPrint("""df.select(kurtosis(col("age")))""")
    df.select(kurtosis(col("age"))).show()

    formatPrint("""df.select(kurtosis("age"))""")
    df.select(kurtosis("age")).show()

    // last
    formatPrint("""df.select(last(col("age"), ignoreNulls = true))""")
    df.select(last(col("age"), ignoreNulls = true)).show()

    formatPrint("""df.select(last("age", ignoreNulls = true))""")
    df.select(last("age", ignoreNulls = true)).show()

    formatPrint("""df.select(last(col("age")))""")
    df.select(last(col("age"))).show()

    formatPrint("""df.select(last("age"))""")
    df.select(last("age")).show()

    // max/mean/min
    formatPrint("""df.select(max(col("age")))""")
    df.select(max(col("age"))).show()

    formatPrint("""df.select(max("age"))""")
    df.select(max("age")).show()

    formatPrint("""df.select(mean(col("age")))""")
    df.select(mean(col("age"))).show()

    formatPrint("""df.select(mean("age"))""")
    df.select(mean("age")).show()

    formatPrint("""df.select(min(col("age")))""")
    df.select(min(col("age"))).show()

    formatPrint("""df.select(min("age"))""")
    df.select(min("age")).show()

    // percentile_approx
    formatPrint("""df.select(percentile_approx(col("age"), lit(0.5), lit(50)))""")
    df.select(percentile_approx(col("age"), lit(0.5), lit(50))).show()

    // product
    formatPrint("""df.select(product(col("age")))""")
    df.select(product(col("age"))).show()

    // skewness
    formatPrint("""df.select(skewness(col("age")))""")
    df.select(skewness(col("age"))).show()

    formatPrint("""df.select(skewness("age"))""")
    df.select(skewness("age")).show()

    // stddev/stddev_samp/stddev_pop
    formatPrint("""df.select(stddev(col("age")))""")
    df.select(stddev(col("age"))).show()

    formatPrint("""df.select(stddev("age"))""")
    df.select(stddev("age")).show()

    formatPrint("""df.select(stddev_samp(col("age")))""")
    df.select(stddev_samp(col("age"))).show()

    formatPrint("""df.select(stddev_samp("age"))""")
    df.select(stddev_samp("age")).show()

    formatPrint("""df.select(stddev_pop(col("age")))""")
    df.select(stddev_pop(col("age"))).show()

    formatPrint("""df.select(stddev_pop("age"))""")
    df.select(stddev_pop("age")).show()

    // sum/sum_distinct
    formatPrint("""df.select(sum(col("age")))""")
    df.select(sum(col("age"))).show()

    formatPrint("""df.select(sum("age"))""")
    df.select(sum("age")).show()

    formatPrint("""df.select(sumDistinct(col("age")))""")
    df.select(sumDistinct(col("age"))).show()

    formatPrint("""df.select(sumDistinct("age"))""")
    df.select(sumDistinct("age")).show()

    formatPrint("""df.select(sum_distinct(col("age")))""")
    df.select(sum_distinct(col("age"))).show()

    // variance/var_samp/var_pop
    formatPrint("""df.select(variance(col("age")))""")
    df.select(variance(col("age"))).show()

    formatPrint("""df.select(variance("age"))""")
    df.select(variance("age")).show()

    formatPrint("""df.select(var_samp(col("age")))""")
    df.select(var_samp(col("age"))).show()

    formatPrint("""df.select(var_samp("age"))""")
    df.select(var_samp("age")).show()

    formatPrint("""df.select(var_pop(col("age")))""")
    df.select(var_pop(col("age"))).show()

    formatPrint("""df.select(var_pop("age"))""")
    df.select(var_pop("age")).show()
  }
}
