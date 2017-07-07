name := "population-data"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

// libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"
// libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.4"
// libraryDependencies += "com.lihaoyi" %% "scalarx" % "0.3.2"

// libraryDependencies += "org.scalacheck" % "scalacheck_2.11" % "1.13.5"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
// libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.6.0" % "test"
