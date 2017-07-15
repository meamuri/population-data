package services

import dao.City
import org.apache.spark.rdd.RDD
import utils.SparkUtils

/**
  *
  */
class Miner {
  val utils = new SparkUtils

  def countCitiesWithPopulationMoreThan(cities: RDD[City], level: Int): RDD[(String, Int)] = {
    val filtered_rows = utils.filterByPopulation(cities, level)
    val res = utils.groupCitiesByCountries(filtered_rows)
    res.mapValues(it => it.size)
  }

  def countriesWithTopN(cities: RDD[City], n: Int): RDD[(String, Iterable[City])] = {
    val countries = utils.groupCitiesByCountries(cities)
    countries.mapValues(cities_of_country => {
      cities_of_country.toList.sortBy(city => city.population).reverse.take(n)
    })
  }

  def countriesPopulation(cities: RDD[City]): RDD[(String, Double)] = {
    cities.map(city => (city.country, city.population))
      .groupByKey()
      .mapValues(all_populations => all_populations.sum)
  }

  def getRatio(cities: RDD[City]): RDD[(String, Double)] = {
    val countries = utils.groupCitiesByCountries(cities)

    countries.mapValues(cities_of_country => {
        var male = 0.0
        var female = 0.0
        for (c <- cities_of_country) {
          if (c.sex == 'm') {
            male = male + c.population
          } else {
            female = female + c.population}
        }
        male / female
      })
  }

}
