package com.shockang.study.spark.core.write

import com.shockang.study.spark.{MYSQL_JDBC_URL, MYSQL_PASS, MYSQL_USER}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object WriteMysqlRDDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WriteMysqlRDDExample")
    val sc = new SparkContext(conf)

    Class.forName("com.mysql.jdbc.Driver")

    val rddData = sc.parallelize(List(("Alice", 30), ("Kotlin", 37)))
    rddData.foreachPartition((iter: Iterator[(String, Int)]) => {
      val conn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASS)
      conn.setAutoCommit(false)
      val preparedStatement = conn.prepareStatement("INSERT INTO spark_examples.person (`name`, `age`) VALUES (?, ?);")
      iter.foreach(t => {
        preparedStatement.setString(1, t._1)
        preparedStatement.setInt(2, t._2)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      conn.commit()
      conn.close()
    })
    sc.stop()
  }
}
