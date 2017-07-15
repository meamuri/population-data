package utils

import dao.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Работа со Spark
  */
class SparkUtils {

  def groupCitiesByCountries(cities: RDD[City]): RDD[(String, Iterable[City])] = {
    cities
      .map(city => (city.country, city))
      .groupByKey()
  }

  def filterByPopulation(cities: RDD[City], levelPer1000: Int): RDD[City] = {
    cities.filter(city => city.population > levelPer1000 * 1000)
  }

} // ... class SparkUtils
