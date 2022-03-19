package com.shockang.study.spark.core

import com.shockang.study.spark.util.Utils
import com.shockang.study.spark.{LocalSparkContext, SparkFunSuite}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.scalatest.BeforeAndAfter

import java.sql._

class JdbcRDDSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.apache.derby.jdbc.EmbeddedDriver")
    val conn = DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb;create=true")
    try {

      try {
        val create = conn.createStatement
        create.execute(
          """
          CREATE TABLE FOO(
            ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
            DATA INTEGER
          )""")
        create.close()
        val insert = conn.prepareStatement("INSERT INTO FOO(DATA) VALUES(?)")
        (1 to 100).foreach { i =>
          insert.setInt(1, i * 2)
          insert.executeUpdate
        }
        insert.close()
      } catch {
        case e: SQLException if e.getSQLState == "X0Y32" =>
        // table exists
      }

      try {
        val create = conn.createStatement
        create.execute("CREATE TABLE BIGINT_TEST(ID BIGINT NOT NULL, DATA INTEGER)")
        create.close()
        val insert = conn.prepareStatement("INSERT INTO BIGINT_TEST VALUES(?,?)")
        (1 to 100).foreach { i =>
          insert.setLong(1, 100000000000000000L + 4000000000000000L * i)
          insert.setInt(2, i)
          insert.executeUpdate
        }
        insert.close()
      } catch {
        case e: SQLException if e.getSQLState == "X0Y32" =>
        // table exists
      }

    } finally {
      conn.close()
    }
  }

  test("basic functionality") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcRDD(
      sc,
      () => {
        DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb")
      },
      "SELECT DATA FROM FOO WHERE ? <= ID AND ID <= ?",
      1, 100, 3,
      (r: ResultSet) => {
        r.getInt(1)
      }).cache()

    assert(rdd.count === 100)
    assert(rdd.reduce(_ + _) === 10100)
  }

  test("large id overflow") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcRDD(
      sc,
      () => {
        DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb")
      },
      "SELECT DATA FROM BIGINT_TEST WHERE ? <= ID AND ID <= ?",
      1131544775L, 567279358897692673L, 20,
      (r: ResultSet) => {
        r.getInt(1)
      }).cache()
    assert(rdd.count === 100)
    assert(rdd.reduce(_ + _) === 5050)
  }

  override def afterAll(): Unit = {
    try {
      DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb;shutdown=true")
    } catch {
      case se: SQLException if se.getSQLState == "08006" =>
      // Normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
    }
    super.afterAll()
  }
}
