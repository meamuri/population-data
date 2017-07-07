package utils

import dao.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

/**
  * Работа со Spark
  */
object SparkUtils {
  def getCitiesWithMillionPopulation(allData: DataFrame): RDD[(Any, List[Any])] = {
    DataUtils.getCitiesWithPopulationMoreThan(all_data = allData, 1000)
  }

  def getCountiesPopulation(allData: DataFrame): RDD[(Any, Any)] = {
    DataUtils.getPopulationOfCountries(allData)
  }

  def getTop5_cities(allData: DataFrame): RDD[(Any, Iterable[List[Any]])] = {
    DataUtils.getTopNByCountries(allData, 5)
  }

  private def selectUsefulData(all_data: DataFrame): RDD[City] = {
    all_data.select("Country or Area", "City", "Year", "Value").rdd
      .map(row => {
        val year = Try(row(2).toString.toInt).getOrElse(-1)
        val population = Try(row(3).toString.toDouble).getOrElse(0.0)
        City(row(0).toString, row(1).toString, year, population)
      })
  }

  private def selectUsefulRows(data: RDD[City], year: Int = -1): RDD[City] = {
    val tmp = data.map(city => (city.name, city))
      .groupByKey()
    val res = if (year == -1) {
        tmp.mapValues(cities => cities.maxBy(_.year))
      } else {
        tmp.mapValues(cities => cities.find(city => city.year == year).getOrElse(cities.head))
      }
    res.map(pair => pair._2)
  }

  def filterByPopulation(cities: RDD[City], level: Int = 1000): RDD[City] = {
    cities.filter(city => city.population > level * 1000)
  }

  def countriesPopulation(cities: RDD[City]): RDD[(String, Double)] = {
    cities.map(city => (city.country, city.population))
      .groupByKey()
      .mapValues(all_populations => all_populations.sum)
  }
}
