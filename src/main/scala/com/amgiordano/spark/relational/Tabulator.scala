package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.annotation.tailrec

object Tabulator {

  type EntityTriplet = (String, DataFrame, Array[String])

  def tabulate(entityName: String, df: DataFrame, foreignKeys: Array[String]): (DataFrame, Array[EntityTriplet]) = {

    val primaryKey = NameComposer.indexName(entityName)
    val allKeys = Array(primaryKey) ++ foreignKeys

    @tailrec
    def go(df: DataFrame): (DataFrame, Array[EntityTriplet]) = {
      if (checkNoObjects(df)) {
        val arrCols = arrayColumns(df)
        (df.drop(arrCols: _*), arrCols.map(colName => tripletFromArrayColumn(df, colName, allKeys)))
      } else {
        go(flattenOneLevel(df))
      }
    }

    go(df)
  }

  def checkNoObjects(df: DataFrame): Boolean = df.schema.count(_.dataType.isInstanceOf[StructType]) == 0

  def flattenOneLevel(df: DataFrame): DataFrame = {
    var dfFlat = df
    for (struct <- dfFlat.schema.filter(_.dataType.isInstanceOf[StructType])) {
      val colName = struct.name
      for (fieldName <- struct.dataType.asInstanceOf[StructType].fields.map(_.name)) {
        dfFlat = dfFlat.withColumn(NameComposer.compose(colName, fieldName), col(s"$colName.$fieldName"))
      }
      dfFlat = dfFlat.drop(colName)
    }
    dfFlat
  }

  def arrayColumns(df: DataFrame): Array[String] = {
    df.schema.filter(_.dataType.isInstanceOf[ArrayType]).map(_.name).toArray
  }

  def tripletFromArrayColumn(df: DataFrame, colName: String, allKeys: Array[String]): EntityTriplet = {
    (colName, explodeArrayColumn(df, colName, allKeys), allKeys)
  }

  def explodeArrayColumn(df: DataFrame, colName: String, allKeys: Array[String]): DataFrame = {
    df.select(allKeys.map(col) ++ Array(explode(col(colName)).as(colName)): _*)
  }
}
