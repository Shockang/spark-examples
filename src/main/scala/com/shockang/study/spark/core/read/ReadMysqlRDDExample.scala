package com.shockang.study.spark.core.read

import com.shockang.study.spark.{MYSQL_JDBC_URL, MYSQL_PASS, MYSQL_USER}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object ReadMysqlRDDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ReadMysqlRDDExample")
    val sc = new SparkContext(conf)

    val inputMysql = new JdbcRDD(sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASS)
      },
      "SELECT * FROM person WHERE id >= ? and id <= ?;",
      1,
      3,
      1,
      r => (r.getInt(1), r.getString(2), r.getInt(3)))

    println("查询到的记录条目数：" + inputMysql.count)
    inputMysql.foreach(println)

    sc.stop()
  }
}
