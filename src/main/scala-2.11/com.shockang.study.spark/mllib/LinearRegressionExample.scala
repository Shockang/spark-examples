package com.shockang.study.spark.mllib

import com.shockang.study.spark.ML_DATA_DIR
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
 * JDK: 8
 * Scala: 2.11.8
 * spark-mllib_2.11: 2.2.0
 *
 * @author Shockang
 */
object LinearRegressionExample {
  val DATA_PATH = ML_DATA_DIR + "lpsa.data"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LinearRegressionExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 读取并创建训练数据
    val data = sc.textFile(DATA_PATH)
    val parsedData = data.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    })
    // 训练模型
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)
    // 预测并统计回归错误的样本比例
    val valuesAndPreds = parsedData.map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })
    val MSE = valuesAndPreds.map {
      case (v, p) => math.pow(v - p, 2)
    }.reduce(_ + _) / valuesAndPreds.count()
    println(s"Training Mean Squared Error = $MSE")
  }
}
