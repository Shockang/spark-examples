package com.shockang.study.spark.core.accumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object CustomAccumulatorExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CustomAccumulatorExample")
    val sc = new SparkContext(conf)

    val userArray = Array(User("Alice", "Female", 11),
      User("Bob", "Male", 16),
      User("Thomas", "Male", 14),
      User("Catalina", "Female", 20),
      User("Boris", "Third", 12),
      User("Karen", "Female", 9),
      User("Tom", "Male", 7))

    val userRDD = sc.parallelize(userArray, 2)

    lazy val userAccumulator = new UserAccumulator[User]
    sc.register(userAccumulator, "自定义用户累加器")

    userRDD.foreach(userAccumulator.add)

    println(userAccumulator)
  }

  case class User(name: String, sex: String, age: Int)

  class UserAccumulator[T] extends AccumulatorV2[T, Array[Int]] {
    //Array(男性, 女性, 第三性别, 12岁以下, 12~18岁)
    private val _array: Array[Int] = Array(0, 0, 0, 0, 0)

    override def isZero: Boolean = _array.mkString("").toLong == 0L

    override def copy(): AccumulatorV2[T, Array[Int]] = {
      val newAcc = new UserAccumulator[T]
      _array.copyToArray(newAcc._array)
      newAcc
    }

    override def reset(): Unit = {
      for (i <- _array.indices) {
        _array(i) = 0
      }
    }

    override def add(v: T): Unit = {
      val user = v.asInstanceOf[User]
      if (user.sex == "Female") {
        _array(0) += 1
      } else if (user.sex == "Male") {
        _array(1) += 1
      } else {
        _array(2) += 1
      }

      if (user.age < 12) {
        _array(3) += 1
      } else if (user.age < 18) {
        _array(4) += 1
      }
    }

    override def merge(other: AccumulatorV2[T, Array[Int]]): Unit = {
      val o = other.asInstanceOf[UserAccumulator[T]]
      _array(0) += o._array(0)
      _array(1) += o._array(1)
      _array(2) += o._array(2)
      _array(3) += o._array(3)
      _array(4) += o._array(4)
    }

    override def value: Array[Int] = {
      _array
    }

    override def toString(): String = {
      getClass.getSimpleName + s"(id: $id, name: $name, value: ${value.mkString(",")})"
    }
  }

}
