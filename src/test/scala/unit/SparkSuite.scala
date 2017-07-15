package unit

import dao.City
import factories.{Resources, SparkFactory}
import helpers.Common
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSuite}
import services.Miner
import utils.DataLoader

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

  test("data-set contain 3 country with million more population") {
    // belarus and brazil -- they have not million more population cities
    val res = cities.filter(city => city.population > 1000*1000)
                  .map(city => (city.country, city))
                  .groupByKey()
    assert(res.count() === 3)
  }

  test("million more search result does not contain belarus record"){
    val res = cities.filter(city => city.population > 1000*1000)
      .map(city => (city.country, city))
    val empty = res.filter(city => city._1 == "Belarus")
    assert(empty.count() === 0)
    val brazil = res.filter(city => city._1 == "Brazil")
    assert(brazil.count() === 0)
  }

  test("my library func works correctly"){
    val worker = new Miner
    val res = worker.countCitiesWithPopulationMoreThan(cities, 1000)
    assert(res.count() === 3)
    val belarus_records = res.filter(pair => pair._1 == "Belarus").count
    assert(belarus_records === 0)
    val australia = res.filter(pair => pair._1 == "Australia")
    assert(australia.count() === 1 && australia.take(1).head._2 === 2)
  }

}
