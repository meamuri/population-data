package initial

import java.io.File

import com.github.tototoshi.csv.CSVReader
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Создание Spark контекста, единого для всей программы
  */
object SparkInit {
  private val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
  private val sc = new SparkContext(conf)

  def getSparkContext: SparkContext = { sc }

  def testSpark(sparkContext: SparkContext): Unit = {
    val logFile = "README.md" // Should be some file on your system
    val logData = sparkContext.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    sparkContext.stop()
  }


}
