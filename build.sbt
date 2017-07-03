name := "population-data"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.4"
libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

// libraryDependencies += "com.lihaoyi" %% "scalarx" % "0.3.2"
// libraryDependencies += "org.scalacheck" % "scalacheck_2.11" % "1.13.5"