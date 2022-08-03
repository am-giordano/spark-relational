package com.amgiordano.spark.relational

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class RelationalSchemaSuite extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder.master("local[1]").getOrCreate
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  def assertSameRS(inputStrings: Seq[String], expectedSchema: Map[String, Seq[String]]): Assertion = {
    val dfInput = spark.read.json(inputStrings.toDS)
    val rs = RelationalSchema(dfInput)
    assert(rs.dataFrames.forall(item => assertSameDataFrames(item._2, spark.read.json(expectedSchema(item._1).toDS))))
  }

  def assertSameDataFrames(dfActual: DataFrame, dfExpected: DataFrame): Boolean = {
    val aCols = dfActual.columns.sorted
    val eCols = dfExpected.columns.sorted
    if (aCols sameElements eCols) aCols.forall(c => values(dfActual, c) sameElements values(dfExpected, c)) else false
  }

  def values(df: DataFrame, colName: String): Array[Any] = df.select(colName).collect.map(_(0))

  test("Flat document") {
    assertSameRS(Seq("{'a': 0}"), Map("root" -> Seq("{'root!!__id__': 0, 'a': 0}")))
  }

  test("With object") {
    assertSameRS(Seq("{'a': {'b': 0}}"), Map("root" -> Seq("{'root!!__id__': 0, 'a!!b': 0}")))
  }

  test("With array") {
    assertSameRS(
      Seq("{'a': [0]}"),
      Map(
        "root" -> Seq("{'root!!__id__': 0}"),
        "a" -> Seq("{'a!!__id__': 0, 'root!!__id__': 0, 'a': 0}")
      )
    )
  }

  test("With array of objects") {
    assertSameRS(
      Seq("{'a': [{'b': 0}]}"),
      Map(
        "root" -> Seq("{'root!!__id__': 0}"),
        "a" -> Seq("{'a!!__id__': 0, 'root!!__id__': 0, 'a!!b': 0}")
      )
    )
  }
}
