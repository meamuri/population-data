package services

import dao.{City, PartOfPeople}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import utils.DataUtils

import scala.util.Try

/**
  *
  */
class Miner(val data: RDD[Map[String, String]]) {
  private val all_data = selectUsefulRows(rowsToCities(data))

  def getCitiesWithMillionPopulation: RDD[(String, Int)] = {
    val filtered_rows = filterByPopulation(cities, 1000)
    val res = groupCitiesByCountries(filtered_rows)
    res.mapValues(it => it.size)
  }

  def getCountiesPopulation: RDD[(String, Double)] = {
    countriesPopulation(cities)
  }

  def getTop5cities: RDD[(String, Iterable[City])] = {
    countriesWithTopN(cities, 5)
  }

  def getRatio(data_by_sexes: DataFrame): RDD[(String, Double)] = {
    val cities = DataUtils.getCitiesBothSexes(data_by_sexes, year)
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

  private def rowsToCities(data: RDD[Map[String, String]]): RDD[City] =
    data.map(row =>  City(
      country = Try(row.get("Country or Area").toString).getOrElse("null").replaceAll("\\.", ""),
      name = Try(row.get("City").toString).getOrElse("null").replaceAll("\\.", ""),
      year = Try(row.get("Year").toString.toInt).getOrElse(-1),
      population = Try(row.get("Value").toString.toDouble).getOrElse(0.0),
      sex = PartOfPeople.strToChar(Try(row.get("Sex").toString).getOrElse("b"))
    ))

  private def selectUsefulRows(data: RDD[City], year: Int = -1): RDD[City] = {
    val tmp = data.map(city => (city.name, city.copy()))
      .groupByKey()
    val res = if (year == -1) {
      tmp.mapValues(cities => cities.maxBy(_.year))
    } else { // если нашли указанный год, берем за этот год, в противном случае самую свежую запись
      tmp.mapValues(cities => cities.find(city => city.year == year).getOrElse(cities.maxBy(_.year)))
    }
    res.map(pair => pair._2.copy())
  }
}
