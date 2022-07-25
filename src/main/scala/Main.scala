import com.amgiordano.spark.relatable.RelationalSchema

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("relatable demo")
      .config("spark.master", "local")
      .getOrCreate

    val df = spark
      .read
      .json("data/example_input.json")

    val rs = new RelationalSchema(df)

    rs.dataFrames.foreach(_._2.show)
  }
}
