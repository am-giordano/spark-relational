package com.amgiordano.spark.relational

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class RelationalSchemaSuite extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder.master("local[1]").getOrCreate
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  def assertSameRS(inputStrings: Seq[String], expectedSchema: Map[String, Seq[String]]): Assertion = {
    val dfInput = spark.read.json(inputStrings.toDS)
    val rs = new RelationalSchema(dfInput)
    assert(rs.dataFrames.forall(item => actualString(item._2) == expectedString(expectedSchema(item._1))))
  }

  def actualString(df: DataFrame): String = df.select(df.columns.sorted.map(col): _*).toString

  def expectedString(jsonStrings: Seq[String]): String = spark.read.json(jsonStrings.toDS).toString

  test("Flat document") {
    assertSameRS(Seq("{'a': 0}"), Map("main" -> Seq("{'main!!__id__': 0, 'a': 0}")))
  }

  test("With object") {
    assertSameRS(Seq("{'a': {'b': 0}}"), Map("main" -> Seq("{'main!!__id__': 0, 'a!!b': 0}")))
  }

  test("With array") {
    assertSameRS(
      Seq("{'a': [{'b': 0}]}"),
      Map(
        "main" -> Seq("{'main!!__id__': 0}"),
        "a" -> Seq("{'a!!__id__': 0, 'main!!__id__': 0, 'a!!b': 0}")
      )
    )
  }
}
