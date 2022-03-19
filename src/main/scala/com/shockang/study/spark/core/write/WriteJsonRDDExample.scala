package com.shockang.study.spark.core.write

import com.shockang.study.spark.WRITE_DATA_DIR
import com.shockang.study.spark.util.Utils.writableLocalFsPath
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.{JSONArray, JSONObject}

object WriteJsonRDDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WriteJsonRDDExample")
    val sc = new SparkContext(conf)

    val map1 = Map("name" -> "Thomas", "age" -> "20", "address" -> JSONArray(List("通信地址1", "通信地址2")))
    val map2 = Map("name" -> "Alice", "age" -> "18", "address" -> JSONArray(List("通信地址1", "通信地址2", "通信地址3")))

    val rddData = sc.parallelize(List(JSONObject(map1), JSONObject(map2)), 1)
    val tsvPath = WRITE_DATA_DIR + "WriteJsonRDDExample"
    rddData.saveAsTextFile(writableLocalFsPath(tsvPath))

    sc.stop()
  }
}
