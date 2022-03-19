package com.shockang.study.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}

object CollectionAccumulatorExample {
  case class User(name: String, telephone: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CollectionAccumulatorExample")
    val sc = new SparkContext(conf)

    val userArray = Array(User("Alice", "15837312345"),
      User("Bob", "13937312666"),
      User("Thomas", "13637312345"),
      User("Tom", "18537312777"),
      User("Boris", "13837312998"))

    val userRDD = sc.parallelize(userArray, 2)
    val userAccumulator = sc.collectionAccumulator[User]("用户累加器")

    userRDD.foreach(user => {
      val telephone = user.telephone.reverse
      if (telephone(0) == telephone(1) && telephone(0) == telephone(2)) {
        userAccumulator.add(user)
      }
    })

    println(userAccumulator)

  }
}
