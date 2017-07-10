package unit

import factories.{MongoFactory, SparkFactory}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
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
  var loader: DataLoader = _
  var all_df: DataFrame = _
  var worker: SparkUtils = _
  var keeper: MongoUtils = _

  before {
    loader = new DataLoader("short-test-data")
    all_df = loader.loadDataWithBothSexes(SparkFactory.getSparkSession)
    worker = new SparkUtils(all_df, -1)
    keeper = new MongoUtils

    all_df.createOrReplaceTempView("population")
  }

  after {
    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }

  test("data file should contain info about 5 countries") {
    val res = all_df.select("Country or Area").distinct()
      .rdd
      .count()
    assert(res === 5)
  }
}
