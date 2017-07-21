package factories

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Создание Spark контекста, единого для всей программы
  */
object SparkFactory {
  private val conf = new SparkConf()
    .setMaster("local")
    .setAppName("dsr")
    .set("spark.cores.max", Resources.getSparkCoreCountConfig)
    .set("spark.master", Resources.getSparkLocalConfig)
    .set("spark.executor.memory", Resources.getSparkMemoryConfig)

  private val sc = new SparkContext(conf)
  private val session = SparkSession.builder()
      .config(conf=conf)
      .appName("dsr")
      .getOrCreate()

  def getSparkContext: SparkContext = { sc }
  def getSparkSession: SparkSession = { session }

  def closeSession(): Unit = { session.close() }
}
