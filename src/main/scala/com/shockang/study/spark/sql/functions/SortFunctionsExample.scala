package com.shockang.study.spark.sql.functions

import com.shockang.study.spark.READ_DATA_DIR
import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 *
 * @author Shockang
 */
object SortFunctionsExample {

  val DATA_PATH = READ_DATA_DIR + "employees.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SortFunctionsExample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.csv(DATA_PATH).toDF("name", "dept", "age", "salary").cache()

    // asc
    formatPrint("""df.sort(asc("dept"), desc("age"))""")
    df.sort(asc("dept"), desc("age")).show()
    formatPrint("""df.sort(asc_nulls_first("dept"), desc("age"))""")
    df.sort(asc_nulls_first("dept"), desc("age")).show()
    formatPrint("""df.sort(asc_nulls_last("dept"), desc("age"))""")
    df.sort(asc_nulls_last("dept"), desc("age")).show()

    //desc
    formatPrint("""df.sort(asc("dept"), desc("age"))""")
    df.sort(asc("dept"), desc("age")).show()
    formatPrint("""df.sort(asc("dept"), desc_nulls_first("age"))""")
    df.sort(asc("dept"), desc_nulls_first("age")).show()
    formatPrint("""df.sort(asc("dept"), desc_nulls_last("age"))""")
    df.sort(asc("dept"), desc_nulls_last("age")).show()
  }
}
