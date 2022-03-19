package com.shockang.study.spark.graphx


import com.shockang.study.spark.GRAPHX_DATA_DIR
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
 * 基于社交网络数据集的 PageRank 示例
 */
object PageRankExample {
  val FOLLOWERS_PATH = GRAPHX_DATA_DIR + "followers.txt"
  val USERS_PATH = GRAPHX_DATA_DIR + "users.txt"

  def main(args: Array[String]): Unit = {

    // 创建 SparkSession
    val spark = SparkSession
      .builder
      .appName("PageRankExample")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // 加载边作为图
    val graph = GraphLoader.edgeListFile(sc, FOLLOWERS_PATH)
    // 运行 PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join ranks with the usernames
    val users = sc.textFile(USERS_PATH).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // 打印结果
    println(ranksByUsername.collect().mkString("\n"))
    spark.stop()
  }
}
