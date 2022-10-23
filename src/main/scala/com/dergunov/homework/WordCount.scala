package com.dergunov.homework

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{concat_ws, explode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.language.postfixOps

object WordCount {
  def main(args : Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark data frame words count")
      .getOrCreate()

    val conf = spark.sparkContext.getConf
    val sqlContext = spark.sqlContext;

    val inputPath = conf.get(Config.IN_PATH_PARAM, Config.IN_PATH_DEFAULT)
    val outputPath = conf.get(Config.OUT_PATH_PARAM, Config.OUT_PATH_DEFAULT)

    val stopWordsStrPath = conf.get(Config.STOP_WORDS_PARAM, Config.STOP_WORDS_DEFAULT)

    import spark.sqlContext.implicits._


    val dfStopWords: DataFrame = getDfStopWords(spark, stopWordsStrPath)

    sqlContext.read
      .option("header", "true")
      .csv(inputPath)
      .select(concat_ws(" ", $"class", $"comment") as "docs")
      .select(functions.split($"docs", "\\s") as "words")
      .select(explode($"words") as "word")
      .select(functions.trim($"word") as "word")
      .select(functions.regexp_replace($"word", "[.,;:\"()!?-]", "") as "word")
      .select(functions.lower($"word") as "word")
      .filter(!($"word" === ""))
      .except(dfStopWords)
      .groupBy($"word")
      .count()
      .select($"count", $"word")
      .orderBy($"count")
      .write.mode("overwrite")
      .csv(outputPath)

    spark.stop()
  }

  private def getDfStopWords(spark: SparkSession, stopWordsStrPath: String): DataFrame = {
    import spark.sqlContext.implicits._

    val customSchema = StructType(Array(StructField("word", StringType, nullable = true)))

    spark.sqlContext.read
      .schema(customSchema)
      .csv(stopWordsStrPath)
      .select(functions.lower($"word") as "word")
      .select(implicitly($"word"))
      .filter(!($"word" === ""))
  }

  object Config {
    val IN_PATH_PARAM: String = "scala.word.count.input"
    val IN_PATH_DEFAULT: String = "/user/hduser/ppkm/*.csv"

    val OUT_PATH_PARAM: String = "scala.word.count.output"
    val OUT_PATH_DEFAULT: String = "/user/hduser/spark2/ppkm_out"

    val STOP_WORDS_PARAM = "scala.word.count.stop.words"
    val STOP_WORDS_DEFAULT = "/user/hduser/ppkm/stopwordv1.txt"
  }
}
