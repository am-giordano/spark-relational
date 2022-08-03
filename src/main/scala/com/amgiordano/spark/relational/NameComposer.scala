package com.amgiordano.spark.relational

private object NameComposer {
  def compose(args: String*): String = args.mkString("!!")
  def indexName(entityName: String): String = compose(entityName, "__id__")
}
