package com.shockang.study.spark.sql.extensions

import com.shockang.study.spark.sql.extensions.SparkSessionExtensionExample.myFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * Spark SQL 自定义扩展
 *
 * @author Shockang
 */
object SparkSessionExtensionExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkSessionExtensionExample")
      .config(SPARK_SESSION_EXTENSIONS.key, classOf[MyExtensions].getCanonicalName)
      .getOrCreate()
    try {
      // planning 阶段使用的策略
      assert(spark.sessionState.planner.strategies.contains(MySparkStrategy(spark)))
      // analysis 阶段 resolution 规则批中使用的规则
      assert(spark.sessionState.analyzer.extendedResolutionRules.contains(MyRule(spark)))
      // analysis 阶段 Post-Hoc Resolution 规则批中使用的规则
      assert(spark.sessionState.analyzer.postHocResolutionRules.contains(MyRule(spark)))
      // analysis 阶段的检测规则
      assert(spark.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(spark)))
      // optimization 阶段使用的规则
      assert(spark.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(spark)))
      // 自定义 parsing 阶段使用的 ParserInterface
      assert(spark.sessionState.sqlParser.isInstanceOf[MyParser])
      // 自定义数据库函数
      assert(spark.sessionState.functionRegistry
        .lookupFunction(myFunction._1).isDefined)
    } finally {
      spark.stop()
    }
  }

  val myFunction: (FunctionIdentifier, ExpressionInfo, Seq[Expression] => Literal) = (FunctionIdentifier("myFunction"),
    new ExpressionInfo(
      "noClass",
      "myDb",
      "myFunction",
      "usage",
      "extended usage",
      "    Examples:",
      """
       note
      """,
      "",
      "3.0.0",
      """
       deprecated
      """,
      ""),
    (_: Seq[Expression]) => Literal(5, IntegerType))

}


class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    // planning 阶段使用的策略
    e.injectPlannerStrategy(MySparkStrategy)
    // analysis 阶段 resolution 规则批中使用的规则
    e.injectResolutionRule(MyRule)
    // analysis 阶段 Post-Hoc Resolution 规则批中使用的规则
    e.injectPostHocResolutionRule(MyRule)
    // analysis 阶段的检测规则
    e.injectCheckRule(MyCheckRule)
    // optimization 阶段使用的规则
    e.injectOptimizerRule(MyRule)
    // 自定义 parsing 阶段使用的 ParserInterface
    e.injectParser(MyParser)
    // 自定义数据库函数
    e.injectFunction(myFunction)
  }
}

case class MySparkStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyCheckRule(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = {}
}

case class MyParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan =
    delegate.parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    delegate.parseQuery(sqlText)
}


