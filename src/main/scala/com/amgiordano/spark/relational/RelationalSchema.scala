package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RelationalSchema(df: DataFrame, rootName: String = "main") {

  type TableMap = mutable.LinkedHashMap[String, DataFrame]
  private type TripletBuffer = ArrayBuffer[(String, DataFrame, Array[String])]

  val dataFrames: TableMap = mutable.LinkedHashMap()
  private val toProcess: TripletBuffer = ArrayBuffer((rootName, df, Array()))

  make()

  private def make(): Unit = {
    while (toProcess.nonEmpty) {
      var (entityName, df, foreignKeys) = toProcess.remove(0)
      while (dataFrames.keySet.contains(entityName)) entityName += "_"
      val (dfNew, fromArrayTriplets) = Tabulator.tabulate(entityName, insertIndex(df, entityName), foreignKeys)
      dataFrames.update(entityName, dfNew)
      toProcess ++= fromArrayTriplets
    }
  }

  private def insertIndex(df: DataFrame, entityName: String): DataFrame = {
    df.select(Array(monotonically_increasing_id().as(NameComposer.indexName(entityName))) ++ df.columns.map(col): _*)
  }
}

object RelationalSchema {
  def apply(df: DataFrame, rootName: String = "root"): RelationalSchema = new RelationalSchema(df, rootName)
}
