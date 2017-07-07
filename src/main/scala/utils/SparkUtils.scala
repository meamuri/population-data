package utils

import dao.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

/**
  * Работа со Spark
  */
class SparkUtils(all_data: DataFrame) {
  private val cities = UsefulInitializer.getCities(all_data)

  def getCitiesWithMillionPopulation: RDD[(String, Iterable[City])] = {
    val filtered_rows = filterByPopulation(cities, 1000)
    groupCitiesByCountries(filtered_rows)
  }

  def getCountiesPopulation: RDD[(String, Double)] = {
    countriesPopulation(cities)
  }

  def getTop5_cities: RDD[(String, Iterable[City])] = {
    countriesWithTopN(cities, 5)
  }

  private def groupCitiesByCountries(cities: RDD[City]): RDD[(String, Iterable[City])] = {
    cities.map(city => (city.country, city)).groupByKey()
  }

  private def filterByPopulation(cities: RDD[City], level: Int = 100): RDD[City] = {
    cities.filter(city => city.population > level * 1000)
  }

  private def countriesPopulation(cities: RDD[City]): RDD[(String, Double)] = {
    cities.map(city => (city.country, city.population))
      .groupByKey()
      .mapValues(all_populations => all_populations.sum)
  }

  private def countriesWithTopN(cities: RDD[City], n: Int = 1): RDD[(String, Iterable[City])] = {
    val countries = cities.map(city => (city.country, city)).groupByKey()
    countries.mapValues(cities_of_country => {
      cities_of_country.toList.sortBy(city => city.population).take(n)
    })
  }
}

private object UsefulInitializer {
  def getCities(all_data: DataFrame): RDD[City] = {
    selectUsefulRows(selectUsefulData(all_data))
  }

  private def selectUsefulData(all_data: DataFrame): RDD[City] = {
    all_data.select("Country or Area", "City", "Year", "Value").rdd
      .map(row => {
        val country = Try(row(0).toString).getOrElse("null")
        val city_name = Try(row(1).toString).getOrElse("null")
        val year = Try(row(2).toString.toInt).getOrElse(-1)
        val population = Try(row(3).toString.toDouble).getOrElse(0.0)
        City(country, city_name, year, population)
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
}


