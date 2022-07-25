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
      .json("data/input/resumes.json")

    val rs = new RelationalSchema(df)

    rs.dataFrames.foreach(
      item => item._2.write.option("header", "true").csv(s"data/output/${item._1}")
    )
  }
}
