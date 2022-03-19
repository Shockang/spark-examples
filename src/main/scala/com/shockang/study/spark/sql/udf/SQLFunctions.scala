package com.shockang.study.spark.sql.udf

import com.shockang.study.spark.READ_DATA_DIR
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

/**
 *
 * @author Shockang
 */
object SQLFunctions {
  def main(args: Array[String]): Unit = {
    //1:初始化SparkContext
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLFunctions")
      .config("truncate", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val jsonDF = spark.read.json(READ_DATA_DIR + "people.json")

    jsonDF.createOrReplaceTempView("people")
    spark.sql("SELECT * FROM people where age > 20").show()

    import spark.implicits._

    val df = Seq(
      ("alex", "shanghai"),
      ("Andy", "beijing")).toDF("name", "city")

    df.show()

    df.createOrReplaceTempView("people_temp")

    spark.sql("select a.*,b.city from people a ,people_temp b where a.name=b.name").show()

    //spark内置函数示例-数组操作
    spark.sql("SELECT array(1, 2, 3);").show() //定义数组
    spark.sql("SELECT array_contains(array(1, 2, 3), 2);").show() //判断数组是否包含元素
    spark.sql("SELECT array_distinct(array(1, 2, 3, null, 3));").show() //数组去除操作
    spark.sql("SELECT  array_except(array(1, 2, 3), array(1, 3, 5));").show() //返回第一个数组中和第二个数组不重复的元素
    spark.sql("SELECT array_intersect(array(1, 2, 3), array(1, 3, 5));").show() //返回两个数组交集-在数组1和数组2都出现的数组
    spark.sql("SELECT array_max(array(1, 20, null, 3));").show() //返回数组中最大的元素
    spark.sql(" SELECT array_min(array(1, 20, null, 3));").show() //返回数组中最小的元素
    spark.sql(" SELECT array_remove(array(1, 2, 3, null, 3), 3);").show() //删除数组中的数据
    spark.sql(" SELECT array_sort(array('b', 'd', null, 'c', 'a'));").show() //数组排序
    spark.sql(" SELECT array_union(array(1, 2, 3), array(1, 3, 5));").show() //数组合并去除重复元素
    spark.sql(" SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4));").show() //数据合并
    spark.sql(" SELECT concat('Spark', 'SQL')").show() //字符串连接
    spark.sql(" SELECT concat(array(1, 2, 3), array(4, 5), array(6));").show() //数组连接
    spark.sql(" SELECT flatten(array(array(1, 2), array(3, 4)));").show() //将多个数组内的数据装载到一个数组中
    spark.sql(" SELECT reverse('Spark SQL');").show() //字符串顺序反转
    spark.sql(" SELECT reverse(array(2, 1, 4, 3));").show() //数组内元素顺序反转
    spark.sql(" SELECT sequence(1, 5);").show() //返回一个从start到end内所有数据组成的一个序列【第一个参数为start，第二个参数为end】
    spark.sql(" SELECT sequence(5, 1);").show() //返回一个从start到end内所有数据组成的一个序列【第一个参数为start，第二个参数为end】
    spark.sql(" SELECT shuffle(array(1, 20, 3, 5));").show() //数组内数据打散（洗牌）
    spark.sql(" SELECT slice(array(1, 2, 3, 4), 2, 2);").show() //数组截取
    spark.sql(" SELECT slice(array(1, 2, 3, 4), -2, 2);").show() //数组截取

    //spark内置函数示例-map操作
    spark.sql("SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c')); ").show() //将2个map进行合并
    spark.sql(" SELECT map_entries(map(1, 'a', 2, 'b'));").show() //将map转为无序数组。
    spark.sql(" SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));").show() //将struct转为Map
    spark.sql(" SELECT map_keys(map(1, 'a', 2, 'b'));").show() //返回Map中的key
    spark.sql(" SELECT map_values(map(1, 'a', 2, 'b'));").show() //返回Map中的value
    //spark内置函数示例-时间相关函数操作
    spark.sql(" SELECT current_date();").show() //获取当前时间
    spark.sql(" SELECT current_timestamp();").show() //获取当前时间戳
    spark.sql("SELECT date_add('2016-07-30', 1); ").show() //给日期加1天
    spark.sql("SELECT date_format('2016-04-08', 'y'); ").show() //获取年份-y:year
    spark.sql(" SELECT date_sub('2016-07-30', 1);").show() //给日期减1天
    spark.sql(" SELECT datediff('2009-07-31', '2009-07-30');").show() //计算2个十日相差多少天
    spark.sql(" SELECT minute('2009-07-30 12:58:59');").show() //获取时间上的分钟
    spark.sql(" SELECT month('2009-07-30 12:58:59');").show() //获取时间上的月
    spark.sql("SELECT now(); ").show() //获取当前时间
    spark.sql(" SELECT to_date('2009-07-30 04:17:52');").show() //字符串转时间类型
    spark.sql(" SELECT to_timestamp('2016-12-31 00:12:00');").show() //字符串转timestamp
    spark.sql("SELECT to_timestamp('2016-12-31', 'yyyy-MM-dd'); ").show() //字符串按照格式转为timestamp
    spark.sql(" SELECT to_unix_timestamp('2016-04-08', 'yyyy-MM-dd');").show() //字符串转unix_time(时间戳)，单位到s
    spark.sql(" SELECT unix_timestamp();").show() //获取当前unix时间戳
    //json Function
    spark.sql(" SELECT from_json('{\"a\":1, \"b\":0.8}', 'a INT, b DOUBLE');").show() //返回JSON对象中的value值
    spark.sql(" SELECT get_json_object('{\"a\":\"b\"}', '$.a');").show() //抽取JSON中对应key上的值，返回值为一个字符串
    spark.sql(" SELECT json_tuple('{\"a\":1, \"b\":2}', 'a', 'b');").show() //抽取JSON中多个key上的值，返回值为一个tuple
    spark.sql(" SELECT to_json(named_struct('a', 1, 'b', 2));").show() //named_struct类型转JSON
    spark.sql(" SELECT to_json(array(named_struct('a', 1, 'b', 2)));").show() //array类型转JSON
    spark.sql(" SELECT to_json(map('a', named_struct('b', 1)));").show() //map类型转JSON
    spark.sql(" SELECT to_json(map(named_struct('a', 1),named_struct('b', 2)));").show() //map类型转JSON
    spark.sql(" SELECT to_json(map('a', 1));").show() //map类型转JSON
    spark.sql(" SELECT to_json(array((map('a', 1))));").show() //array类型转JSON

    //
    spark.sql(" SELECT count(name),name from people group by name order by name desc").show()
    spark.sql(" SELECT sum(col) FROM VALUES (NULL), (10), (15) AS tab(col);").show()
    spark.sql(" SELECT max(col) FROM VALUES (NULL), (10), (15) AS tab(col);").show()
    spark.sql("  SELECT min(col) FROM VALUES (10), (-1), (20) AS tab(col);").show()
    spark.sql("  SELECT count(DISTINCT col) FROM VALUES (NULL), (5), (5), (10) AS tab(col);").show()
    spark.sql("  SELECT count(col) FROM VALUES (NULL), (5), (5), (20) AS tab(col);").show()


    val plusOne = udf((x: Int) => x + 1)
    spark.udf.register("plusOne", plusOne)
    spark.sql("select age , plusOne(age) age_1,name from people").show()


    def age_plus(age: Int, step: Int): Int = {
      age + step
    }

    spark.udf.register("age_plus", (age: Int, step: Int) => {
      age + step
    })

    spark.sql("select age , age_plus(age,10) age_1,name from people").show()

    spark.sql("SELECT hash('Spark');").show()

    // Register the function to access it
    spark.udf.register("myAverage", functions.udaf(MyAverage))

    spark.sql("SELECT myAverage(age) as age_ave FROM people").show()
  }

  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Long, Average, Double] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Average = Average(0L, 0L)

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Average, data: Long): Average = {
      buffer.sum += data
      buffer.count += 1
      buffer
    }

    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    // Transform the output of the reduction
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    // Specifies the Encoder for the intermediate value type
    //   def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble

    def bufferEncoder: Encoder[Average] = Encoders.product[Average]
  }

}
