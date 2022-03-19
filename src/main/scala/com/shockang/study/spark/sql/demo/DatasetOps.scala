package com.shockang.study.spark.sql.demo

import com.shockang.study.spark.READ_DATA_DIR
import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author Shockang
 */
object DatasetOps {

  case class Person(name: String, age: Long)

  case class Score(n: String, score: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DatasetOps").master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    /**
     * Dataset中的tranformation和Action操作，Action类型的操作有：
     * show collect first reduce take count等
     * 这些操作都会产生结果，也就是说会执行逻辑计算的过程
     */
    val personSchema = StructType(List(
      StructField("name", StringType, false),
      StructField("age", LongType, true)
    ))
    val personsDF = spark.read.schema(personSchema).json(READ_DATA_DIR + "people.json")
    val scoreSchema = StructType(List(
      StructField("n", StringType, false),
      StructField("score", LongType, false)
    ))
    val personScoresDF = spark.read.schema(scoreSchema).csv(READ_DATA_DIR + "peopleScores.csv")
    val personsDS = personsDF.as[Person].cache()
    val personScoresDS = personScoresDF.as[Score].cache()

    personsDS.groupBy($"name", $"age").agg(concat($"name", $"age")).show


    personsDS.groupBy($"name").agg(sum($"age"), avg($"age"), max($"age"), min($"age"), count($"age")
      , countDistinct($"age"), mean($"age"), current_date()).show

    personsDS.groupBy($"name")
      .agg(collect_list($"name"), collect_set($"name"))
      .collect().foreach(println)

    formatPrint("sample")
    personsDS.sample(false, 0.5).show()

    formatPrint("randomSplit")
    personsDS.randomSplit(Array(10, 20)).foreach(dataset => dataset.show())

    formatPrint("joinWith")
    personsDS.joinWith(personScoresDS, $"name" === $"n").show

    formatPrint("sort")
    personsDS.sort($"age".desc).show

    formatPrint("map")
    personsDS.map { person =>
      (person.name, person.age + 100L)
    }.show()

    formatPrint("mapPartitions")
    personsDS.mapPartitions { persons =>
      val result = ArrayBuffer[(String, Long)]()
      while (persons.hasNext) {
        val person = persons.next()
        result += ((person.name, person.age + 1000))
      }
      result.iterator
    }.show

    formatPrint("dropDuplicates")
    personsDS.dropDuplicates("name").show()

    formatPrint("distinct")
    personsDS.distinct().show()

    formatPrint("personsDS.rdd.partitions.size")
    println(personsDS.rdd.partitions.size)

    val repartitionedDS = personsDS.repartition(4)
    formatPrint("repartitionedDS.rdd.partitions.size")
    println(repartitionedDS.rdd.partitions.size)

    val coalesced = repartitionedDS.repartition(2)
    formatPrint("coalesced.rdd.partitions.size")
    println(coalesced.rdd.partitions.size)

    repartitionedDS.show()

    formatPrint("filter and join and groupBy and agg")
    personsDF.filter("age > 20").join(personScoresDF, $"name" === $"n").
      groupBy(personsDF("name")).agg(avg(personScoresDF("score")), avg(personsDF("age"))).show()

    personsDF.show()

    formatPrint("collect")
    personsDF.collect().foreach(println)

    formatPrint("count")
    println(personsDF.count())

    formatPrint("first")
    println(personsDF.first())

    formatPrint("take")
    personsDF.take(2).foreach(println)

    formatPrint("foreach")
    personsDF.foreach(item => println(item))

    formatPrint("map and reduce")
    println(personsDF.map(person => 1).reduce(_ + _))

    val personsDS2 = personsDF.as[Person]
    personsDS2.show()

    personsDS2.printSchema()

    personsDF.createOrReplaceTempView("persons")

    spark.sql("SELECT * FROM persons WHERE age >20").explain()

    spark.stop()

  }
}
