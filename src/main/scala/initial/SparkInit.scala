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

}
