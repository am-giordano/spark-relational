ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark-relational"
  )

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.3.0"
