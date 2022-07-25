import com.amgiordano.spark.relatable.RelationalSchema

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val filename = args(0)
    val mainEntityName = args(1)

    val spark = SparkSession
      .builder
      .appName("spark-relatable")
      .config("spark.master", "local")
      .getOrCreate

    val df = spark
      .read
      .json(s"data/input/$filename")

    val rs = new RelationalSchema(df, mainEntityName)

    rs.dataFrames.foreach(
      item => item._2
        .write
        .option("header", "true")
        .csv(s"data/output/${filename.replace(".json", "")}/${item._1}")
    )
  }
}
