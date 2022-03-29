package com.shockang.study.spark.sql.show

import com.shockang.study.spark.SQL_DATA_DIR
import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.spark.sql.SparkSession

/**
 *
 * @author Shockang
 */
object ShowExample {

  val DATA_PATH: String = SQL_DATA_DIR + "user.json"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ShowExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.read.json(DATA_PATH).createTempView("t_user")

    val df = spark.sql("SELECT * FROM t_user")

    formatPrint("""df.show""")
    df.show

    formatPrint("""df.show(2)""")
    df.show(2)

    formatPrint("""df.show(true)""")
    df.show(true)
    formatPrint("""df.show(false)""")
    df.show(false)

    formatPrint("""df.show(2, truncate = true)""")
    df.show(2, truncate = true)
    formatPrint("""df.show(2, truncate = false)""")
    df.show(2, truncate = false)

    formatPrint("""df.show(2, truncate = 0)""")
    df.show(2, truncate = 0)
    formatPrint("""df.show(2, truncate = 20)""")
    df.show(2, truncate = 20)

    formatPrint("""df.show(2, truncate = 0, vertical = true)""")
    df.show(2, truncate = 0, vertical = true)
    formatPrint("""df.show(2, truncate = 20, vertical = true)""")
    df.show(2, truncate = 20, vertical = true)
    formatPrint("""df.show(2, truncate = 0, vertical = false)""")
    df.show(2, truncate = 0, vertical = false)
    formatPrint("""df.show(2, truncate = 20, vertical = false)""")
    df.show(2, truncate = 20, vertical = false)

    spark.stop()
  }
}
