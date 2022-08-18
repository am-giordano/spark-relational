package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.annotation.tailrec

/**
 * Contains the tabulate function and it's helper functions.
 */
object Tabulator {

  /**
   * Type of (entity name, DataFrame, foreign keys) triplets.
   */
  type EntityTriplet = (String, DataFrame, Array[String])

  /**
   * Tail-recursive function for flattening a DataFrame until the are no StructType columns and extracting all ArrayType columns, if
   * any, and creating DataFrames for each of these after exploding.
   *
   * @param entityName Name of the entity represented by the DataFrame.
   * @param df DataFrame to process.
   * @param foreignKeys Array of foreign keys included in the DataFrame.
   * @return Tuple composed by the flattened DataFrame and the EntityTriplets resulting from extracting the ArrayType
   *         columns.
   */
  def tabulate(entityName: String, df: DataFrame, foreignKeys: Array[String]): (DataFrame, Array[EntityTriplet]) = {

    val primaryKey = NameComposer.identifierName(entityName)
    val allKeys = primaryKey +: foreignKeys

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

  /**
   * Function for checking whether there are or not StructType columns in a DataFrame.
   *
   * @param df DataFrame for which to check StructType columns.
   * @return True if there aren't any StructType columns, false otherwise.
   */
  def checkNoObjects(df: DataFrame): Boolean = df.schema.count(_.dataType.isInstanceOf[StructType]) == 0

  /**
   * Function for doing a single flattening step on a DataFrame with StructType columns. This means generating
   * additional columns from the fields of each of the found StructType columns and removing the latter.
   *
   * @param df DataFrame to flatten.
   * @return Partially-flattened DataFrame (if there were nested StructTypes, there will still be StructType columns).
   */
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

  /**
   * Function for obtaining the ArrayType column names of a DataFrame.
   *
   * @param df DataFrame for which to obtain these column names.
   * @return Array of column names of the ArrayType columns of the DataFrame.
   */
  def arrayColumns(df: DataFrame): Array[String] = {
    df.schema.filter(_.dataType.isInstanceOf[ArrayType]).map(_.name).toArray
  }

  /**
   * Function for creating an EntityTriplet from an ArrayType column. This is done by exploding the column and forming
   * the triplet with column name as entity name and the array of all keys in the original entity as the foreign keys
   * of the new entity.
   *
   * @param df DataFrame containing the column to explode.
   * @param colName Name of the ArrayType column to explode.
   * @param allKeys Array of keys in df.
   * @return EntityTriplet for the ArrayType exploded column.
   */
  def tripletFromArrayColumn(df: DataFrame, colName: String, allKeys: Array[String]): EntityTriplet = {
    (colName, explodeArrayColumn(df, colName, allKeys), allKeys)
  }

  /**
   * Function for creating a DataFrame resulting from exploding an ArrayType column, including all the other columns.
   *
   * @param df DataFrame containing the column to explode.
   * @param colName Name of the ArrayType column to explode.
   * @param allKeys Array of keys in df.
   * @return DataFrame with the column exploded.
   */
  def explodeArrayColumn(df: DataFrame, colName: String, allKeys: Array[String]): DataFrame = {
    df.select(allKeys.map(col) :+ explode(col(colName)).as(colName): _*)
  }
}
