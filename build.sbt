name := "population-data"

version := "1.0"

scalaVersion := "2.11.8"

// библиотеки Spark + MongoDB
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

// библиотеки тестирования
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"

// библиотеки чтения конфигурационных файлов
libraryDependencies += "com.typesafe" % "config" % "1.3.1"

// чтение аргументов командной строки:
libraryDependencies += "com.github.scopt" % "scopt_2.10" % "3.6.0"

