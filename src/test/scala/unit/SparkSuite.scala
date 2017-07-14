package unit

import dao.City
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Тесты модулей приложения.
  * Используют файлы
  * test_both.csv
  * test_diff.scv
  * для проверки работоспособностей модулей.
  * Файлы содержат сильно укороченный набор исходных файлов
  */
class SparkSuite extends FunSuite with BeforeAndAfter {
  var cities: RDD[City] = _
  var countries: RDD[(String, Iterable[City])] = _

  before {
  }

  after {

  }

  test("data file should contain info about 5 countries") {
  }

  test("algorithm should take me info about 5 countries") {
  }

  test("only 3 countries should have cities-millionaires"){
  }

  test("data should contain only must recently information, if year = -1") {
  }

  test("data_loader should check, that folder contain data files") {
  }

}
