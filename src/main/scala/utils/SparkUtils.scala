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

  def getCitiesWithBothData: RDD[City] = selectBothRows(rowsToCities(both), loader.getYear)
  def getCitiesWithDiffData: RDD[City] = selectDiffRows(rowsToCities(diff), loader.getYear)

  def filterCities(cities: RDD[City], level: Int): RDD[City] =
    cities.filter(city => city.population > level)

  def collectByCountries(cities: RDD[City]): RDD[(String, Iterable[City])] =
    cities.map(city => (city.country, city)).groupByKey()

  private def rowsToCities(data: RDD[Map[String, String]]): RDD[City] = {
    data.collect().take(5).foreach(p => println(p))
    val res = data.map(row => {
      val city = City(
        country = row.getOrElse("Country or Area", "null").replaceAll("\\.", ""),
        name = row.getOrElse("City", "null").replaceAll("\\.", ""),
        year = row.getOrElse("Year", "-1").toInt,
        population = row.getOrElse("Value", "0.0").toDouble,
        sex = PartOfPeople.strToChar(row.getOrElse("Sex", "b"))
      )
      city
    })
//    res.collect().take(5).foreach(p => println(p))
    res
  }

  private def selectUsefulRows(data: RDD[City], year: Int): RDD[City] = {
    val tmp = data.map(city => (city.name, city.copy()))
      .groupByKey()
    val res = if (year == -1) {
      tmp.mapValues(cities => cities.maxBy(_.year))
    } else { // если нашли указанный год, берем за этот год, в противном случае самую свежую запись
      tmp.mapValues(cities => cities.find(city => city.year == year).getOrElse(cities.maxBy(_.year)))
    }
    res.map(pair => pair._2.copy())
  }

  private def selectBothRows(data: RDD[City], year: Int): RDD[City] = {
    selectUsefulRows(data, year)
  }

  private def selectDiffRows(data: RDD[City], year: Int): RDD[City] =
    selectUsefulRows(data.filter(city => city.sex == 'm'), year)
      .union(selectUsefulRows(data.filter(city => city.sex == 'f'), year))

} // ... class SparkUtils
