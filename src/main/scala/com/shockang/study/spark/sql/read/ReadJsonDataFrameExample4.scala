package com.shockang.study.spark.sql.read

import com.shockang.study.spark.READ_DATA_DIR
import org.apache.spark.sql.SparkSession

/**
 *
 * @author Shockang
 */
object ReadJsonDataFrameExample4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ReadJsonDataFrameExample4")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //生成DataFrame集合，读取并打印所有用户数据，统计拥有多少位用户。然后找到所有女性用户中，年龄小于25岁的用户。
    val df1 = spark.read.json(READ_DATA_DIR + "people.json")
    df1.createTempView("t_user")
    df1.createOrReplaceTempView("t_user")

    df1.createGlobalTempView("t_user")
    df1.createOrReplaceGlobalTempView("t_user")

    spark.newSession().sql("SELECT * FROM global_temp.t_user").show
    spark.sql("SELECT * FROM global_temp.t_user").show
    spark.sql("SELECT name, age, sex FROM global_temp.t_user WHERE sex = 'Female' AND age < 25").show
    spark.sql("SELECT sex, MAX(age), COUNT(name) FROM global_temp.t_user GROUP BY sex").show

  }
}
