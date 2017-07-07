package utils

import dao.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

/**
  * Работа со Spark
  */
class SparkUtils(all_data: DataFrame) {
  private val cities = DataUtils.getCities(all_data)

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

//  def getRatio(data_by_sexes: DataFrame): RDD[(String, Double)] = {
//
//  }

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
