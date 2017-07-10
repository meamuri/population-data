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
  val loader: DataLoader = new DataLoader("short-test-data")
  val all_df: DataFrame = loader.loadDataWithBothSexes(SparkFactory.getSparkSession)
  val worker: SparkUtils = new SparkUtils(all_df, -1)
  val keeper: MongoUtils = new MongoUtils

  before {

  }

  after {

  }

  test("data file should contain info about 5 countries") {
    val res = all_df.select("Country or Area").distinct()
      .rdd
      .count()
    assert(res === 5)
  }

  test("data_loader should check, that folder contain data files") {
    val some_loader = new DataLoader("abc")
    assert(!some_loader.checkWorkFolder())
    assert(loader.checkWorkFolder())
  }
}
