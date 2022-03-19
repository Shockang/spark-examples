package com.shockang.study.spark.ml.california_housing

import com.shockang.study.spark._
import com.shockang.study.spark.util.Utils.formatStr
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.language.implicitConversions

/**
 * 处理加州房屋信息，构建线性回归模型
 *
 * 参考 https://time.geekbang.org/column/article/98374
 *
 * @author Shockang
 */
object CaliforniaHousingApps {
  val DATA_PATH = ML_DATA_DIR + "CaliforniaHousing/cal_housing.data"
  val DOMAIN_PATH = ML_DATA_DIR + "CaliforniaHousing/cal_housing.domain"

  val SCHEMA = "longitude,latitude,housingMedianAge,totalRooms,totalBedRooms,population,households,medianIncome,medianHouseValue"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("spark").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("CaliforniaHousingApps").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    // 读取数据并创建 RDD
    val rdd = sc.textFile(DATA_PATH)
    // 读取数据每个属性的定义并创建 RDD
    val header = sc.textFile(DOMAIN_PATH)
    println(formatStr("了解数据集"))
    printArray(header.collect())
    printArray(rdd.take(2))
    printArray(rdd.map(line => line.split(",")).take(2))

    //使用 Struct 方式把 data 的数据格式化,即在 RDD 的基础上增加数据的元数据信息
    val schema = StructType(SCHEMA.split(",")
      .map(column => StructField(column, DoubleType)))
    // 把我们的每一条数据变成以 Row 为单位的数据
    val rows = rdd.map(_.split(","))
      .map(line => line.map(_.toDouble))
      .map(line => Row(line(0),
        line(1), line(2), line(3), line(4),
        line(5), line(6), line(7), line(8)))
    var df = spark.createDataFrame(rows, schema)
    df.show()

    import spark.implicits._

    // 统计出所有建造年限各有多少个房子
    df.groupBy("housingMedianAge").count().sort($"housingMedianAge".desc).show()

    println(formatStr("预处理"))
    // 房价的值普遍都很大，我们可以把它调整成相对较小的数字
    df = df.withColumn("medianHouseValue", $"medianHouseValue" / 100000)

    // 有的属性没什么意义，比如所有房子的总房间数和总卧室数，我们更加关心的是平均房间数
    // 我们可以添加如下三个新的列：
    // 每个家庭的平均房间数：roomsPerHousehold；
    // 每个家庭的平均人数：populationPerHousehold；
    // 卧室在总房间的占比：bedroomsPerRoom
    df = df.withColumn("roomsPerHousehold", $"totalRooms" / $"households")
      .withColumn("populationPerHousehold", $"population" / $"households")
      .withColumn("bedroomsPerRoom", $"totalBedRooms" / $"totalRooms")

    // 同样，有的列是我们并不关心的，比如经纬度，这个数值很难有线性的意义。所以我们可以只留下重要的信息列。
    df = df.select("medianHouseValue",
      "totalBedRooms",
      "population",
      "households",
      "medianIncome",
      "roomsPerHousehold",
      "populationPerHousehold",
      "bedroomsPerRoom")

    // 在我们想要构建的线性模型中，房价是结果，其他属性是输入参数。所以我们需要把它们分离处理；
    val inputData = df.rdd.map(x => (x.toSeq.head.asInstanceOf[Double],
      new DenseVector(x.toSeq.tail.toArray.map(_.asInstanceOf[Double]))))

    df = inputData.toDF("label", "features")

    // 我们创建了一个 StandardScaler，它的输入是 features 列，输出被我们命名为 features_scaled。
    val standardScaler = new StandardScaler().setInputCol("features").setOutputCol("features_scaled")
    val scaler = standardScaler.fit(df)
    val scaledDF = scaler.transform(df)
    // 我们把这个 scaler 对已有的 DataFrame 进行处理，让我们看下代码块里显示的输出结果。
    printArray(scaledDF.take(1))

    println(formatStr("创建模型"))
    // 开始构建线性回归模型
    // 首先，我们需要把数据集分为训练集和测试集，训练集用来训练模型，测试集用来评估模型的正确性。
    // DataFrame 的 randomSplit() 函数可以很容易的随机分割数据，这里我们将 80% 的数据用于训练，剩下 20% 作为测试集。
    val array = scaledDF.randomSplit(Array(0.8, 0.2), 123)

    val trainData = array(0)
    val testData = array(1)
    // 用 Spark ML 提供的 LinearRegression 功能，我们可以很容易得构建一个线性回归模型
    val lr = new LinearRegression().setFeaturesCol("features_scaled").setLabelCol("label").setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val linearModel = lr.fit(trainData)

    // 现在有了模型，我们终于可以用 linearModel 的 transform() 函数来预测测试集中的房价，并与真实情况进行对比
    val predicted = linearModel.transform(testData)
    val predictions = predicted.select("prediction").rdd.map(x => x(0))
    val labels = predicted.select("label").rdd.map(x => x(0))
    val predictionAndLabel = predictions.zip(labels).collect()

    // 我们用 RDD 的 zip() 函数把预测值和真实值放在一起，这样可以方便地进行比较。比如让我们看一下前两个对比结果。
    printArray(predictionAndLabel.take(2))
  }
}
