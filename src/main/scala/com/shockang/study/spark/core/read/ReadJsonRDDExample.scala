package com.shockang.study.spark.core.read

import com.shockang.study.spark.READ_DATA_DIR
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object ReadJsonRDDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ReadJsonRDDExample")
    val sc = new SparkContext(conf)

    val inputJsonFile = sc.textFile(READ_DATA_DIR + "people.json")

    val content = inputJsonFile.map(JSON.parseFull)
    println(content.collect.mkString("\t"))

    content.foreach(
      {
        case Some(map: Map[String, Any]) => println(map)
        case None => println("无效的Json")
        case _ => println("其他异常")
      }
    )

    sc.stop()
  }
}
