package unit

import dao.City
import factories.SparkFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSuite}
import utils.{DataLoader, DataUtils, MongoUtils, SparkUtils}

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

  var cities: RDD[City] = _
  var countries: RDD[(String, Iterable[City])] = _

  before {
    cities = DataUtils.getCities(all_df)
    countries = cities.map(city => (city.country, city)).groupByKey()
  }

  after {

  }

  test("data file should contain info about 5 countries") {
    val res = all_df.select("Country or Area").distinct()
      .rdd
      .count()
    assert(res === 5)
  }

  test("algorithm should take me info about 5 countries") {
    val countries_pop = worker.getCountiesPopulation
    assert(countries_pop.count() === 5)
    val countries_top = worker.getTop5_cities
    assert(countries_top.count() === 5)

    val pre = loader.loadDataWithDiffSexes(SparkFactory.getSparkSession)
    val ratio = worker.getRatio(pre)
    assert(ratio.count() === 5)
  }

  test("only 3 countries should have cities-millionaires"){
    val millionaires = worker.getCitiesWithMillionPopulation
    assert(millionaires.count() === 3)
  }

  test("data should contain only must recently information, if year = -1") {
    val armenia = countries.filter(pair => pair._1 == "Armenia")
    assert(armenia.count() === 1)
    val res = armenia.first()
    val cities_of_armenia = res._2.toArray

    assert(cities_of_armenia.length === 2)
    val check_city =
      if (cities_of_armenia(0).name == "Gyumri (Leninakan)") {
        cities_of_armenia(0)
      } else {
        cities_of_armenia(1)
      }
    assert(check_city.year === 2011)
  }

  test("data_loader should check, that folder contain data files") {
    val some_loader = new DataLoader("abc")
    assert(!some_loader.checkWorkFolder())
    assert(loader.checkWorkFolder())
  }

}
