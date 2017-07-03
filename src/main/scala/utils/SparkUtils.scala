package utils

import org.apache.spark.SparkContext

/**
  * Работа со Spark
  */
object SparkUtils {
  def testSpark(sparkContext: SparkContext): Unit = {
    val logFile = "README.md" // Should be some file on your system
    val logData = sparkContext.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    //    sparkContext.stop()
  }
}