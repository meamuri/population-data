package unit

import dao.City
import factories.{Resources, SparkFactory}
import helpers.Common
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSuite}
import utils.{DataLoader, DataUtils, MongoUtils, SparkUtils}

import scala.util.Try

/**
  * Тесты модулей приложения.
  * Используют файлы
  * test_both.csv
  * test_diff.scv
  * для проверки работоспособностей модулей.
  * Файлы содержат сильно укороченный набор исходных файлов
  */
class SparkSuite extends FunSuite with BeforeAndAfter {
  val fileBoth: String = Resources.getTestDataPath + Resources.getBothFilename
  val fileDiff: String =  Resources.getTestDataPath + Resources.getDiffFilename
  val loader = new DataLoader

  var dataFrame: DataFrame = _
  var cities: RDD[City] = _

  before {
    dataFrame = loader.loadData(fileBoth, SparkFactory.getSparkSession)
    cities = loader.selectBothRows(dataFrame, -1)
  }

  after {

  }

  test("data file should contain info about 16 cities") {
    val res = cities.count()
    assert(res === 16)
  }

  test("cities rdd with difference year param should be difference"){
    cities = loader.selectBothRows(dataFrame, -1)
    val recently = cities.filter(city => city.name == "Gyumri (Leninakan)")
    val res = recently.collect()
    assert(res.length === 1)
    assert(res(0).year === 2011)

    cities = loader.selectBothRows(dataFrame, 2009)
    val target_year = cities.filter(city => city.name == "Gyumri (Leninakan)")
    val target = target_year.collect()
    assert(target.length === 1)
    assert(target(0).year === 2009)
  }


}
