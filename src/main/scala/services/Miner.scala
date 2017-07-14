package services

import dao.City
import org.apache.spark.rdd.RDD
import utils.{DataLoader, SparkUtils}

/**
  *
  */
class Miner(val loader : DataLoader) {
  private val utils = new SparkUtils(loader)
  private val cities = utils.getCitiesWithBothData
  private val diff = utils.getCitiesWithDiffData

  def getCitiesWithPopulationMoreThan(level: Int): RDD[(String, Int)] = {
    val filtered_rows = utils.filterCities(cities, level * 1000)
    val citiesByCountries = utils.collectByCountries(filtered_rows)
    citiesByCountries.mapValues(it => it.size)
  }

  def getCountiesPopulation: RDD[(String, Double)] = {
    cities.map(city => (city.country, city.population))
      .groupByKey()
      .mapValues(all_populations => all_populations.sum)
  }

  def getTopCities(n: Int): RDD[(String, Iterable[City])] = {
    val countries = utils.collectByCountries(cities)
    countries.mapValues(cities_of_country => {
      cities_of_country.toList.sortBy(city => city.population).take(n)
    })
  }

  def getRatio(isMaleToFemaleRatio: Boolean): RDD[(String, Double)] = {
    val countries = utils.collectByCountries(diff)
    countries.mapValues(cities_of_country => {
      var male = 0.0
      var female = 0.001
      val letter = if (isMaleToFemaleRatio) { 'm' } else { 'f' }
      for (c <- cities_of_country) {
        if (c.sex == letter) {
          male = male + c.population
        } else {
          female = female + c.population}
      }
      val res = if (isMaleToFemaleRatio) { male / female } else { female / male }
      res
    })
  }

} // ... class Miner
