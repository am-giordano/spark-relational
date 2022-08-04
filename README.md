# spark-relational

__spark-relational__ is a Spark package for converting a DataFrame with StructType and ArrayType columns into a set of 
DataFrames with flat columns interrelated by foreign keys.

## Use cases

1. Development of an ETL from a MongoDB to a relational database, such as PostgreSQL.
2. Performing an exploratory data analysis (EDA) on a document-oriented dataset.

## Add as a dependency to your project

### spark-shell, pyspark, or spark-submit

```
> $SPARK_HOME/bin/spark-shell --packages am-giordano:spark-relational:0.3.0
```

### sbt

```
resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"

libraryDependencies += "am-giordano" % "spark-relational" % "0.3.0"
```

### Maven

```
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>am-giordano</groupId>
    <artifactId>spark-relational</artifactId>
    <version>0.3.0</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>https://repos.spark-packages.org/</url>
  </repository>
</repositories>
```

## Example of use: Read-Convert-Write

```
TL;DR

scala> import com.amgiordano.spark.relational.Converter.makeRelationalSchema      
scala> import org.apache.spark.sql.SparkSession
scala> val spark = SparkSession.builder.getOrCreate
scala> val df = spark.read.json("data/input/resumes.json")
scala> val rs = makeRelationalSchema(df, "person")
scala> rs.foreach(item => item._2.write.option("header", "true").csv(s"data/output/resumes/${item._1}"))
```

This example shows how to load a JSON file into a DataFrame, convert it into an interrelated set of flat DataFrames, 
and write these to CSV files. The dataset used can be found in the GitHub repository of this package in 
`data/input/resumes.json`.

Each document in this dataset has a complex structure with nested objects and arrays.

First off, let's instantiate a SparkSession, read the JSON file, and look at its schema:

```
scala> import org.apache.spark.sql.SparkSession
scala> val spark = SparkSession.builder.getOrCreate
scala> val df = spark.read.json("data/input/resumes.json")
scala> df.printSchema
```

Output:

```
root
 |-- age: long (nullable = true)
 |-- experience: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- company: string (nullable = true)
 |    |    |-- from: long (nullable = true)
 |    |    |-- responsibilities: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |-- role: string (nullable = true)
 |    |    |-- technologies: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- primary: boolean (nullable = true)
 |    |    |-- to: long (nullable = true)
 |-- name: string (nullable = true)
```

To generate a relational schema for this dataset, let's call the makeRelationalSchema function passing as arguments 
the DataFrame and a name for the main entity of the dataset, "person":

```
scala> import com.amgiordano.spark.relational.Converter.makeRelationalSchema      
scala> val rs = makeRelationalSchema(df, "person")
```

Now we can look at each of the tables:

```
scala> for ((tableName, df) <- rs) {
     |   println(tableName)
     |   df.show
     | }
```

Output:

```
person
+--------------+---+-----+
|person!!__id__|age| name|
+--------------+---+-----+
|             0| 34|Alice|
|             1| 27|  Bob|
+--------------+---+-----+

experience
+------------------+--------------+-------------------+----------------+--------------------+--------------+
|experience!!__id__|person!!__id__|experience!!company|experience!!from|    experience!!role|experience!!to|
+------------------+--------------+-------------------+----------------+--------------------+--------------+
|                 0|             0|             Google|            2020|   Software Engineer|          2022|
|                 1|             0|           Facebook|            2017|Senior Data Scien...|          2020|
|                 2|             1|             OpenAI|            2019|        NLP Engineer|          2022|
+------------------+--------------+-------------------+----------------+--------------------+--------------+

experience!!responsibilities
+------------------------------------+------------------+--------------+----------------------------------+
|experience!!responsibilities!!__id__|experience!!__id__|person!!__id__|experience!!responsibilities!!name|
+------------------------------------+------------------+--------------+----------------------------------+
|                                   0|                 0|             0|                      Google stuff|
|                                   1|                 0|             0|              Mark TensorFlow i...|
|                                   2|                 1|             0|                      Censor media|
|                                   3|                 1|             0|              Learn the foundat...|
|                                   4|                 1|             0|              Do Kaggle competi...|
|                                   5|                 2|             1|              Assert that GPT-2...|
|                                   6|                 2|             1|              Assert that GPT-3...|
|                                   7|                 2|             1|              Develop a prototy...|
+------------------------------------+------------------+--------------+----------------------------------+

experience!!technologies
+--------------------------------+------------------+--------------+------------------------------+---------------------------------+
|experience!!technologies!!__id__|experience!!__id__|person!!__id__|experience!!technologies!!name|experience!!technologies!!primary|
+--------------------------------+------------------+--------------+------------------------------+---------------------------------+
|                               0|                 0|             0|                           C++|                             true|
|                               1|                 0|             0|                       LolCode|                            false|
|                               2|                 1|             0|                        Python|                             true|
|                               3|                 1|             0|                         Excel|                            false|
|                               4|                 2|             1|                        Triton|                             true|
|                               5|                 2|             1|                         LaTeX|                            false|
+--------------------------------+------------------+--------------+------------------------------+---------------------------------+
```

Finally, let's write each DataFrame to a CSV file:

```
scala> for ((tableName, df) <- rs) {
     |   df.write.option("header", "true").csv(s"data/output/resumes/$tableName")
     | }
```
