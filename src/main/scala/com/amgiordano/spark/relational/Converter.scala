package com.amgiordano.spark.relational

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Contains the main function of spark-relational, makeRelationalSchema.
 */
object Converter {

  /**
   * Type of the output of makeRelationalSchema.
   */
  type RelationalSchema = mutable.LinkedHashMap[String, DataFrame]

  /**
   * Function for converting a DataFrame with StructType and ArrayType columns into a set of DataFrames with flat
   * columns.
   *
   * @param df DataFrame to convert.
   * @param rootName Optional name for the root entity of the underlying documents. Defaults to "root".
   * @return Mutable linked hashmap representing a relational schema for the dataset.
   */
  def makeRelationalSchema(df: DataFrame, rootName: String = "root"): RelationalSchema = {

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

  /**
   * Function for inserting a monotonically increasing identifier to a DataFrame, which will serve as a primary key of
   * DataFrame in the relational schema and will be featured as a foreign key in other DataFrames of the schema.
   *
   * @param df DataFrame in which to insert the identifier.
   * @param entityName Name of the entity represented by the DataFrame, used for naming the identifier column.
   * @return DataFrame with the identifier column.
   */
  def insertIndex(df: DataFrame, entityName: String): DataFrame = {
    df.select(monotonically_increasing_id().as(NameComposer.identifierName(entityName)) +: df.columns.map(col): _*)
  }
}
