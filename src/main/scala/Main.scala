import com.amgiordano.spark.relational.RelationalSchema

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val filename = args(0)
    val mainEntityName = args(1)

    val spark = SparkSession
      .builder
      .appName("spark-relational")
      .config("spark.master", "local")
      .getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    val df = spark
      .read
      .json(s"data/input/$filename")

    df.printSchema

    val rs = new RelationalSchema(df, mainEntityName)

    rs.dataFrames.foreach(
      item => {
        println(item._1)
        val directory = s"data/output/${filename.replace(".json", "")}/${item._1}"
        item._2.write.option("header", "true").csv(directory)
      }
    )
  }
}
