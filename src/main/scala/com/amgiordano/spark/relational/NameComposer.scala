package com.amgiordano.spark.relational

/**
 * Contains functions for naming the columns in the relational schema.
 */
object NameComposer {

  /**
   * Function for composing multiple strings into a single string separated by "!!". Used for generating column names
   * in the relational schema.
   *
   * @param args Strings to compose.
   * @return Composed string.
   */
  def compose(args: String*): String = args.mkString("!!")

  /**
   * Function for composing an identifier column name as "{entityName}!!&#95;&#95;id&#95;&#95;".
   *
   * @param entityName Name of the entity for which to generate the identifier column name.
   * @return Identifier column name for the entity.
   */
  def identifierName(entityName: String): String = compose(entityName, "__id__")
}
