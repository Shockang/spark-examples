package com.shockang.study.spark.sql.functions

import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 *
 * @author Shockang
 */
object WindowFunctionsExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AggregateFunctionsExample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value").cache()
    val w = Window.partitionBy("value").orderBy("key")

    // cume_dist
    formatPrint("""df.select(cume_dist().over(w))""")
    df.select(cume_dist().over(w)).show()

    // dense_rank
    formatPrint("""df.select(dense_rank().over(w))""")
    df.select(dense_rank().over(w)).show()

    // lag/lead
    formatPrint("""df.select(lag($"key", 2).over(w))""")
    df.select(lag($"key", 2).over(w)).show()

    formatPrint("""df.select(lag("key", 2).over(w))""")
    df.select(lag("key", 2).over(w)).show()

    formatPrint("""df.select(lag($"key", 2, "0").over(w))""")
    df.select(lag($"key", 2, "0").over(w)).show()

    formatPrint("""df.select(lag("key", 2, "0").over(w))""")
    df.select(lag("key", 2, "0").over(w)).show()

    formatPrint("""df.select(lag($"key", 2, "0", true).over(w))""")
    df.select(lag($"key", 2, "0", true).over(w)).show()

    formatPrint("""df.select(lead($"key", 2).over(w))""")
    df.select(lead($"key", 2).over(w)).show()

    formatPrint("""df.select(lead("key", 2).over(w))""")
    df.select(lead("key", 2).over(w)).show()

    formatPrint("""df.select(lead($"key", 2, "0").over(w))""")
    df.select(lead($"key", 2, "0").over(w)).show()

    formatPrint("""df.select(lead("key", 2, "0").over(w))""")
    df.select(lead("key", 2, "0").over(w)).show()

    formatPrint("""df.select(lead($"key", 2, "0", true).over(w))""")
    df.select(lead($"key", 2, "0", true).over(w)).show()

    // nth_value
    formatPrint("""df.select(nth_value($"key", 2, true).over(w))""")
    df.select(nth_value($"key", 2, true).over(w)).show()

    formatPrint("""df.select(nth_value($"key", 2).over(w))""")
    df.select(nth_value($"key", 2).over(w)).show()

    // ntile
    formatPrint("""df.select(ntile(2).over(w)).""")
    df.select(ntile(2).over(w)).show()

    // percent_rank
    formatPrint("""df.select(percent_rank().over(w))""")
    df.select(percent_rank().over(w)).show()

    // rank
    formatPrint("""df.select(rank().over(w))""")
    df.select(rank().over(w)).show()

    // row_number
    formatPrint("""df.select(row_number().over(w))""")
    df.select(row_number().over(w)).show()
  }
}
