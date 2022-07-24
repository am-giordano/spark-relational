package com.amgiordano.spark.relatable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RelationalSchema(dfMain: DataFrame, mainEntityName: String = "main") {

  val dataFrames: mutable.LinkedHashMap[String, DataFrame] = mutable.LinkedHashMap()
  private val toProcess: ArrayBuffer[(String, DataFrame, Array[String])] = ArrayBuffer((mainEntityName, dfMain, Array()))

  def make: RelationalSchema = {
    while (toProcess.nonEmpty) {
      var (entityName, df, foreignKeys) = toProcess.remove(0)
      while (dataFrames.keySet.contains(entityName)) entityName += "_"
      val tab = new Tabulator(entityName, insertIndex(df, entityName), foreignKeys) // TODO: put insertIndex logic inside Tabulator
      val (dfNew, fromArrayTriplets) = tab.tabulate
      dataFrames.update(entityName, dfNew)
      toProcess ++= fromArrayTriplets
    }
    this
  }

  private def insertIndex(df: DataFrame, entityName: String): DataFrame = {
    df.select(Array(monotonically_increasing_id().as(s"$entityName!__id__")) ++ df.columns.map(col): _*)
  }
}
