package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

class Tabulator(entityName: String, var df: DataFrame, foreignKeys: Array[String]) {

  type TripletArray = Array[(String, DataFrame, Array[String])]

  private val primaryKey: String = s"$entityName!__id__"
  private val allKeys: Array[String] = Array(primaryKey) ++ foreignKeys

  def tabulate(): (DataFrame, TripletArray) = {
    if (checkNoObjects) {
      val fromArrayTriplets = arrayColumns.map(colName => (colName, extractArrayColumn(colName), allKeys))
      (df, fromArrayTriplets)
    } else {
      flattenOneLevel()
      tabulate()
    }
  }

  private def checkNoObjects: Boolean = df.schema.count(_.dataType.isInstanceOf[StructType]) == 0

  private def flattenOneLevel(): Unit = {
    for (struct <- df.schema.filter(_.dataType.isInstanceOf[StructType])) {
      val colName = struct.name
      struct.dataType.asInstanceOf[StructType].fields.map(_.name).foreach(
        fieldName => df = df.withColumn(s"$colName!$fieldName", col(s"$colName.$fieldName"))
      )
      df = df.drop(colName)
    }
  }

  private def arrayColumns: Array[String] = df.schema.filter(_.dataType.isInstanceOf[ArrayType]).map(_.name).toArray

  private def extractArrayColumn(colName: String): DataFrame = {
    val dfNew = df.select(allKeys.map(col) ++ Array(explode(col(colName)).as(colName)): _*)
    df = df.drop(colName)
    dfNew
  }
}
