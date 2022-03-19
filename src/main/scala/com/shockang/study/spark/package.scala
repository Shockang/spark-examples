package com.shockang.study

/**
 * 存放 spark 包下的一些公共资源
 *
 * @author Shockang
 */
package object spark {
  // 当前用户工作目录
  val USER_DIR = System.getProperty("user.dir")
  // 存放数据文件的目录
  val DATA_DIR = USER_DIR + "/data/"
  // 存放 spark core 数据文件的目录
  val CORE_DATA_DIR = DATA_DIR + "core/"
  // 读取数据文件目录
  val READ_DATA_DIR = CORE_DATA_DIR + "read/"
  // 写入数据文件目录
  val WRITE_DATA_DIR = CORE_DATA_DIR + "write/"
  // 读取 spark sql 数据文件的目录
  val SQL_DATA_DIR = DATA_DIR + "sql/"
  // 存放 spark graphx 数据文件的目录
  val GRAPHX_DATA_DIR = DATA_DIR + "graphx/"
  // 存放 spark ml/mllib 数据文件的目录
  val ML_DATA_DIR = DATA_DIR + "ml/"

  // HDFS 地址
  val HDFS_URL = "node1"
  // HDFS 路径
  val HDFS_PATH = s"hdfs://${HDFS_URL}:9000/"
  // HDFS 数据地址
  val HDFS_DATA_PATH = HDFS_PATH + "data/"
  // HDFS 读取数据文件目录
  val HDFS_READ_DIR = HDFS_DATA_PATH + "read/"
  // HDFS 写入数据文件目录
  val HDFS_WRITE_DIR = HDFS_DATA_PATH + "write/"
  // Spark checkpoint 路径
  val CHECKPOINT_DIR = HDFS_PATH + "spark_checkpoint/"

  // 用户查询日志(SogouQ)版本：2008——精简版(一天数据，63MB)
  val SOGOU_DATA_PATH = READ_DATA_DIR + "SogouQ.reduced"

  // mysql
  val MYSQL_JDBC_URL = "jdbc:mysql://node1:3306/spark_examples?useUnicode=true&characterEncoding=utf8"
  val MYSQL_SCHEMA = "spark_examples"
  val MYSQL_DB_TABLE = ""
  val MYSQL_USER = "root"
  val MYSQL_PASS = "1qaz@WSX"

  // hive
  val HIVE_URL = ""


  // kafka
  val KAFKA_URL = ""
  val KAFKA_TOPIC = ""

  def arrayToString[T](array: Array[T]): String = array.mkString("[", ", ", "]")

  def printArray[T](a: Array[T]): Unit = println(arrayToString(a))
}
