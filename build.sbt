name := "spark-relational"
version := "0.2.0"
organization := "com.amgiordano.spark"
scalaVersion := "2.12.15"
publishMavenStyle := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3"
)
