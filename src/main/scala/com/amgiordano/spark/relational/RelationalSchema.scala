package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object RelationalSchema {

  type RelationalSchema = mutable.LinkedHashMap[String, DataFrame]

  def make(df: DataFrame, rootName: String = "root"): RelationalSchema = {

    val schema = mutable.LinkedHashMap[String, DataFrame]()
    val toProcess = ArrayBuffer[Tabulator.EntityTriplet]((rootName, df, Array()))

    while (toProcess.nonEmpty) {
      val triplet = toProcess.remove(0)
      var entityName = triplet._1
      while (schema.keySet.contains(entityName)) entityName += "_"
      val (df, fromArrayTriplets) = Tabulator.tabulate(entityName, insertIndex(triplet._2, entityName), triplet._3)
      schema.update(entityName, df)
      toProcess ++= fromArrayTriplets
    }

    schema
  }

  def insertIndex(df: DataFrame, entityName: String): DataFrame = {
    df.select(Array(monotonically_increasing_id().as(NameComposer.indexName(entityName))) ++ df.columns.map(col): _*)
  }
}
