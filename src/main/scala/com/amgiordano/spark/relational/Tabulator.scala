package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

private class Tabulator(entityName: String, var df: DataFrame, foreignKeys: Array[String]) {

  type TripletArray = Array[(String, DataFrame, Array[String])]

  val primaryKey: String = NameComposer.indexName(entityName)
  val allKeys: Array[String] = Array(primaryKey) ++ foreignKeys

  def tabulateGo(): (DataFrame, TripletArray) = {
    if (checkNoObjects) {
      val fromArrayTriplets = arrayColumns.map(colName => (colName, extractArrayColumn(colName), allKeys))
      (df, fromArrayTriplets)
    } else {
      flattenOneLevel()
      tabulateGo()
    }
  }

  def checkNoObjects: Boolean = df.schema.count(_.dataType.isInstanceOf[StructType]) == 0

  def flattenOneLevel(): Unit = {
    for (struct <- df.schema.filter(_.dataType.isInstanceOf[StructType])) {
      val colName = struct.name
      for (fieldName <- struct.dataType.asInstanceOf[StructType].fields.map(_.name)) {
        df = df.withColumn(NameComposer.compose(colName, fieldName), col(s"$colName.$fieldName"))
      }
      df = df.drop(colName)
    }
  }

  def arrayColumns: Array[String] = df.schema.filter(_.dataType.isInstanceOf[ArrayType]).map(_.name).toArray

  def extractArrayColumn(colName: String): DataFrame = {
    val dfNew = df.select(allKeys.map(col) ++ Array(explode(col(colName)).as(colName)): _*)
    df = df.drop(colName)
    dfNew
  }
}

object Tabulator {

  type TripletArray = Array[(String, DataFrame, Array[String])]

  def tabulate(entityName: String, df: DataFrame, foreignKeys: Array[String]): (DataFrame, TripletArray) = {
    val tab = new Tabulator(entityName, df, foreignKeys)
    tab.tabulateGo()
  }
}
