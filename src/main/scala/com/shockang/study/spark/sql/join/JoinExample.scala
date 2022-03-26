package com.shockang.study.spark.sql.join

import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.spark.sql.SparkSession

/**
 *
 * Spark SQL 支持的 JOIN 测试
 *
 * @author Shockang
 */
object JoinExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JoinExample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.createDataFrame(List(
      ("Shockang", "程序员"),
      ("Tom", "猫"),
      ("Jerry", "老鼠")
    )).toDF("name", "occupation").cache()

    df1.createTempView("t1")

    val df2 = spark.createDataFrame(List(
      ("Alice", 12),
      ("Tom", 10),
      ("Jerry", 11)
    )).toDF("name", "age").cache()

    df2.createTempView("t2")

    // INNER JOIN
    formatPrint("""df1.join(df2, Seq("name"), joinType = "inner").show()""")
    df1.join(df2, Seq("name"), joinType = "inner").show()

    formatPrint("""spark.sql("SELECT * FROM t1 INNER JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 INNER JOIN t2 ON t1.name=t2.name").show()
    formatPrint("""spark.sql("SELECT * FROM t1 JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 JOIN t2 ON t1.name=t2.name").show()

    // FULL OUTER JOIN
    formatPrint("""df1.join(df2, Seq("name"), joinType = "outer").show()""")
    df1.join(df2, Seq("name"), joinType = "outer").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "full").show()""")
    df1.join(df2, Seq("name"), joinType = "full").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "fullouter").show()""")
    df1.join(df2, Seq("name"), joinType = "fullouter").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "full_outer").show()""")
    df1.join(df2, Seq("name"), joinType = "full_outer").show()

    formatPrint("""spark.sql("SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.name=t2.name").show()
    formatPrint("""spark.sql("SELECT * FROM t1 FULL JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 FULL JOIN t2 ON t1.name=t2.name").show()

    // LEFT OUTER JOIN
    formatPrint("""df1.join(df2, Seq("name"), joinType = "leftouter").show()""")
    df1.join(df2, Seq("name"), joinType = "leftouter").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "left").show()""")
    df1.join(df2, Seq("name"), joinType = "left").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "left_outer").show()""")
    df1.join(df2, Seq("name"), joinType = "left_outer").show()

    formatPrint("""spark.sql("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.name=t2.name").show()
    formatPrint("""spark.sql("SELECT * FROM t1 LEFT JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 LEFT JOIN t2 ON t1.name=t2.name").show()

    // RIGHT OUTER JOIN
    formatPrint("""df1.join(df2, Seq("name"), joinType = "rightouter").show()""")
    df1.join(df2, Seq("name"), joinType = "rightouter").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "right").show()""")
    df1.join(df2, Seq("name"), joinType = "right").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "right_outer").show()""")
    df1.join(df2, Seq("name"), joinType = "right_outer").show()

    formatPrint("""spark.sql("SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.name=t2.name").show()
    formatPrint("""spark.sql("SELECT * FROM t1 RIGHT JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 RIGHT JOIN t2 ON t1.name=t2.name").show()

    // LEFT SEMI JOIN
    formatPrint("""df1.join(df2, Seq("name"), joinType = "leftsemi").show()""")
    df1.join(df2, Seq("name"), joinType = "leftsemi").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "left_semi").show()""")
    df1.join(df2, Seq("name"), joinType = "left_semi").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "semi").show()""")
    df1.join(df2, Seq("name"), joinType = "semi").show()

    formatPrint("""spark.sql("SELECT * FROM t1 LEFT SEMI JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 LEFT SEMI JOIN t2 ON t1.name=t2.name").show()
    formatPrint("""spark.sql("SELECT * FROM t1 SEMI JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 SEMI JOIN t2 ON t1.name=t2.name").show()

    // LEFT ANTI JOIN
    formatPrint("""df1.join(df2, Seq("name"), joinType = "leftanti").show()""")
    df1.join(df2, Seq("name"), joinType = "leftanti").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "left_anti").show()""")
    df1.join(df2, Seq("name"), joinType = "left_anti").show()
    formatPrint("""df1.join(df2, Seq("name"), joinType = "anti").show()""")
    df1.join(df2, Seq("name"), joinType = "anti").show()

    formatPrint("""spark.sql("SELECT * FROM t1 LEFT ANTI JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 LEFT ANTI JOIN t2 ON t1.name=t2.name").show()
    formatPrint("""spark.sql("SELECT * FROM t1 ANTI JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 ANTI JOIN t2 ON t1.name=t2.name").show()

    // CROSS JOIN
    formatPrint("""df1.join(df2, Seq("name"), joinType = "cross").show()""")
    df1.join(df2, Seq("name"), joinType = "cross").show()

    formatPrint("""spark.sql("SELECT * FROM t1 CROSS JOIN t2 ON t1.name=t2.name").show()""")
    spark.sql("SELECT * FROM t1 CROSS JOIN t2 ON t1.name=t2.name").show()

    spark.stop()
  }
}
