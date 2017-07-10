package unit

import factories.{MongoFactory, SparkFactory}
import org.scalatest.{BeforeAndAfter, FunSuite}
import utils.{DataLoader, MongoUtils, SparkUtils}

/**
  * Тесты модулей приложения.
  * Используют файлы
  * test_both.csv
  * test_diff.scv
  * для проверки работоспособностей модулей.
  * Файлы содержат сильно укороченный набор исходных файлов
  */
class SparkSuite extends FunSuite with BeforeAndAfter {

  before {
    val loader = new DataLoader("data")
    val all_df = loader.loadDataWithBothSexes(SparkFactory.getSparkSession)
    val worker = new SparkUtils(all_df, -1)
    val keeper = new MongoUtils
  }

  after {
    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }

  test("spark.sql loads row data from file (DataFrame)") {

  }
}
