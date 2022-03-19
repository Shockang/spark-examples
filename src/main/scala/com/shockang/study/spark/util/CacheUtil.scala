package com.shockang.study.spark.util

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author Shockang
 */
object CacheUtil {
  def cacheSize(spark: SparkSession, df: DataFrame): Unit = {
    val logical = df.queryExecution.logical
    spark.sessionState.executePlan(logical).optimizedPlan.stats.sizeInBytes
  }
}
