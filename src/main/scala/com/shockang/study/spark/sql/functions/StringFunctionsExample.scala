package com.shockang.study.spark.sql.functions

import com.shockang.study.spark.util.Utils.formatPrint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 *
 * @author Shockang
 */
object StringFunctionsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StringFunctionsExample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = Seq(("abc", "aaaBb", "")).toDF("a", "b", "c")
    // ascii
    formatPrint("""df.select(ascii($"a"), ascii($"b"), ascii($"c")).show()""")
    df.select(ascii($"a"), ascii($"b"), ascii($"c")).show()

    // base64
    formatPrint("""df.select(base64($"a"), base64($"b"), base64($"c")).show()""")
    df.select(base64($"a"), base64($"b"), base64($"c")).show()

    // concat_ws
    formatPrint("""df.select(concat_ws(";", $"a", $"b", $"c")).show()""")
    df.select(concat_ws(";", $"a", $"b", $"c")).show()

    // decode/encode
    formatPrint("""df.select(decode($"a", "utf-8")).show()""")
    df.select(decode($"a", "utf-8")).show()

    formatPrint("""df.select(encode($"a", "utf-8")).show()""")
    df.select(encode($"a", "utf-8")).show()

    // format_number/format_string
    formatPrint("""df.select(format_number(lit(5L), 4)).show()""")
    df.select(format_number(lit(5L), 4)).show()

    formatPrint("""df.select(format_number(lit(1.toByte), 4)).show()""")
    df.select(format_number(lit(1.toByte), 4)).show()

    formatPrint("""df.select(format_number(lit(2.toShort), 4)).show()""")
    df.select(format_number(lit(2.toShort), 4)).show()

    formatPrint("""df.select(format_number(lit(3.1322.toFloat), 4)).show()""")
    df.select(format_number(lit(3.1322.toFloat), 4)).show()

    formatPrint("""df.select(format_number(lit(4), 4)).show()""")
    df.select(format_number(lit(4), 4)).show()

    formatPrint("""df.select(format_number(lit(5L), 4)).show()""")
    df.select(format_number(lit(5L), 4)).show()

    formatPrint("""df.select(format_number(lit(6.48173), 4)).show()""")
    df.select(format_number(lit(6.48173), 4)).show()

    formatPrint("""df.select(format_number(lit(BigDecimal("7.128381")), 4)).show()""")
    df.select(format_number(lit(BigDecimal("7.128381")), 4)).show()

    formatPrint("""df.select(format_string("aa%d%s", lit(123), lit("cc"))).show()""")
    df.select(format_string("aa%d%s", lit(123), lit("cc"))).show()

    // initcap
    formatPrint("""df.select(initcap($"a"), initcap($"b"), initcap($"c")).show()""")
    df.select(initcap($"a"), initcap($"b"), initcap($"c")).show()

    // instr
    formatPrint("""df.select(instr($"b", "aa")).show()""")
    df.select(instr($"b", "aa")).show()

    // length
    formatPrint("""df.select(length($"a"), length($"b"), length($"c")).show()""")
    df.select(length($"a"), length($"b"), length($"c")).show()

    // lower
    formatPrint("""df.select(lower($"b")).show()""")
    df.select(lower($"b")).show()

    // levenshtein
    formatPrint("""df.select(levenshtein($"a", $"b")).show()""")
    df.select(levenshtein($"a", $"b")).show()

    // locate
    formatPrint("""df.select(locate("aa", $"b")).show()""")
    df.select(locate("aa", $"b")).show()

    formatPrint("""df.select(locate("aa", $"b", 2)).show()""")
    df.select(locate("aa", $"b", 2)).show()

    // lpad
    formatPrint("""df.select(lpad($"a", 10, " ")).show()""")
    df.select(lpad($"a", 10, " ")).show()

    // ltrim
    formatPrint("""df.select(ltrim(lit("   123"))).show()""")
    df.select(ltrim(lit("   123"))).show()

    formatPrint("""df.select(ltrim(lit("aaa123"), "a")).show()""")
    df.select(ltrim(lit("aaa123"), "a")).show()

    // regexp_extract/regexp_replace
    formatPrint("""df.select(regexp_extract(lit("abc123"), "(\\d+)", 1)).show()""")
    df.select(regexp_extract(lit("abc123"), "(\\d+)", 1)).show()

    formatPrint("""df.select(regexp_replace(lit("abc123"), "(\\d+)", "num")).show()""")
    df.select(regexp_replace(lit("abc123"), "(\\d+)", "num")).show()

    formatPrint("""df.select(regexp_replace(lit("abc123"), lit("(\\d+)"), lit("num"))).show()""")
    df.select(regexp_replace(lit("abc123"), lit("(\\d+)"), lit("num"))).show()

    // unbase64
    formatPrint("""df.select(unbase64(typedlit(Array[Byte](1, 2, 3, 4)))).show()""")
    df.select(unbase64(typedlit(Array[Byte](1, 2, 3, 4)))).show()

    // rpad
    formatPrint("""df.select(rpad($"a", 10, " ")).show()""")
    df.select(rpad($"a", 10, " ")).show()

    // repeat
    formatPrint("""df.select(repeat($"a", 3)).show()""")
    df.select(repeat($"a", 3)).show()

    // rtrim
    formatPrint("""df.select(rtrim(lit("123   "))).show()""")
    df.select(rtrim(lit("123   "))).show()

    formatPrint("""df.select(rtrim(lit("123aaa"), "a")).show()""")
    df.select(rtrim(lit("123aaa"), "a")).show()

    // soundex
    formatPrint("""df.select(soundex($"a"), soundex($"b")).show()""")
    df.select(soundex($"a"), soundex($"b")).show()

    // split
    formatPrint("""df.select(split(lit("a;b;c"), ";")).show()""")
    df.select(split(lit("a;b;c"), ";")).show()

    formatPrint("""df.select(split(lit("a;b;c"), ";", 2)).show()""")
    df.select(split(lit("a;b;c"), ";", 2)).show()

    formatPrint("""df.select(split(lit("a;b;c"), ";", 0)).show()""")
    df.select(split(lit("a;b;c"), ";", 0)).show()

    formatPrint("""df.select(split(lit("a;b;c"), ";", -1)).show()""")
    df.select(split(lit("a;b;c"), ";", -1)).show()

    // substring/substring_index
    formatPrint("""df.select(substring(lit("abcdef"), 2, 5)).show()""")
    df.select(substring(lit("abcdef"), 2, 5)).show()

    formatPrint("""df.select(substring_index(lit("www.shockang.com"), ".", 2)).show()""")
    df.select(substring_index(lit("www.shockang.com"), ".", 2)).show()

    // overlay
    formatPrint("""df.select(overlay(lit("abcdef"), lit("abc"), lit(4), lit(1))).show()""")
    df.select(overlay(lit("abcdef"), lit("abc"), lit(4), lit(1))).show()

    formatPrint("""df.select(overlay(lit("abcdef"), lit("abc"), lit(4))).show()""")
    df.select(overlay(lit("abcdef"), lit("abc"), lit(4))).show()

    // sentences
    formatPrint("""df.select(sentences(lit("我们都有一个家，名字叫中国"), lit("zh"), lit("CN"))).show()""")
    df.select(sentences(lit("我们都有一个家，名字叫中国"), lit("zh"), lit("CN"))).show()

    formatPrint("""df.select(sentences(lit("我们都有一个家，名字叫中国"))).show()""")
    df.select(sentences(lit("我们都有一个家，名字叫中国"))).show()

    // translate
    formatPrint("""df.select(translate(lit("abcdef"), "def", "123")).show()""")
    df.select(translate(lit("abcdef"), "def", "123")).show()

    // trim
    formatPrint("""df.select(trim(lit("   abc   "))).show()""")
    df.select(trim(lit("   abc   "))).show()

    formatPrint("""df.select(trim(lit("aaabcaaaa"), "a")).show()""")
    df.select(trim(lit("aaabcaaaa"), "a")).show()

    // upper
    formatPrint("""df.select(upper($"b")).show()""")
    df.select(upper($"b")).show()
  }
}
