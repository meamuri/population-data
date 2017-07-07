package utils

import dao.PartOfPeople.PartOfPeople
import dao.{City, PartOfPeople}
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

  def getRatio(data_by_sexes: DataFrame): RDD[(String, Double)] = {
    val cities = DataUtils.getCitiesBothSexes(data_by_sexes)
    cities.map(city => (city.country, city))
      .groupByKey()
      .mapValues(cities_of_country => {
        var male = 0.0
        var female = 0.0
        for (c <- cities_of_country) {
          if (c.sex == 'm') {
            male = male + c.population
          } else{
            female = female + c.population}
        }
        male / female
      })
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
