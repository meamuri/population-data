name := "population-data"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"


// libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
