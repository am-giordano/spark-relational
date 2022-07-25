package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RelationalSchema(dfMain: DataFrame, mainEntityName: String = "main") {

  type TableMap = mutable.LinkedHashMap[String, DataFrame]
  type TripletBuffer = ArrayBuffer[(String, DataFrame, Array[String])]

  val dataFrames: TableMap = mutable.LinkedHashMap()
  private val toProcess: TripletBuffer = ArrayBuffer((mainEntityName, dfMain, Array()))

  make()

  def make(): Unit = {
    while (toProcess.nonEmpty) {
      var (entityName, df, foreignKeys) = toProcess.remove(0)
      while (dataFrames.keySet.contains(entityName)) entityName += "_"
      val tab = new Tabulator(entityName, insertIndex(df, entityName), foreignKeys)
      val (dfNew, fromArrayTriplets) = tab.tabulate()
      dataFrames.update(entityName, dfNew)
      toProcess ++= fromArrayTriplets
    }
  }

  private def insertIndex(df: DataFrame, entityName: String): DataFrame = {
    df.select(Array(monotonically_increasing_id().as(NameComposer.indexName(entityName))) ++ df.columns.map(col): _*)
  }
}
