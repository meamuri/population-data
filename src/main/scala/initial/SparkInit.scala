package initial

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Создание Spark контекста, единого для всей программы
  */
object SparkInit {
  private val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
  private val sc = new SparkContext(conf)
  private val session = SparkSession.builder()
      .config(conf=conf)
      .appName("spark session")
      .getOrCreate()

  def getSparkContext: SparkContext = { sc }
  def getSparkSession: SparkSession = { session }

}
