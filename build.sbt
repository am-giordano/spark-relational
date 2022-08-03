name := "spark-relational"
version := "0.3.0"
organization := "com.amgiordano"
description := "Document-oriented to relational data conversion in Spark"
scalaVersion := "2.12.15"
publishMavenStyle := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3" % "test"
)
