package utils

import dao.{City, PartOfPeople}
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * Работа со Spark
  */
class SparkUtils (val loader: DataLoader) {
  private val both = loader.loadDataWithBothSexes
  private val diff = loader.loadDataWithDiffSexes

  def getCitiesWithBothData: RDD[City] = selectBothRows(rowsToCities(both))
  def getCitiesWithDiffData: RDD[City] = selectDiffRows(rowsToCities(diff))

  def filterCities(cities: RDD[City], level: Int): RDD[City] =
    cities.filter(city => city.population > level)

  def collectByCountries(cities: RDD[City]): RDD[(String, Iterable[City])] =
    cities.map(city => (city.country, city)).groupByKey()

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

  private def selectBothRows(data: RDD[City], year: Int = -1): RDD[City] =
    selectUsefulRows(data, year)

  private def selectDiffRows(data: RDD[City], year: Int = -1): RDD[City] =
    selectUsefulRows(data.filter(city => city.sex == 'm'), year)
      .union(selectUsefulRows(data.filter(city => city.sex == 'f'), year))

} // ... class SparkUtils
