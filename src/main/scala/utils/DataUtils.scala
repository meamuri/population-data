package utils

import dao.{City, PartOfPeople}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

import scala.util.Try

/**
  * Модуль для работы с данными
  */
private object DataUtils {
  def getCities(all_data: DataFrame, year: Int = -1): RDD[City] = {
    selectUsefulRows(selectUsefulData(all_data), year)
  }

  def getCitiesBothSexes(all_data: DataFrame, year: Int = -1): RDD[City] = {
    val cities = selectUsefulData(all_data)
    usefulRowsForBothSexes(cities, year)
  }

  private def selectUsefulData(all_data: DataFrame): RDD[City] = {
    all_data.select("Country or Area", "City", "Year", "Value", "Sex").rdd
      .map(row => {
        val country = Try(row(0).toString).getOrElse("null").replaceAll("\\.", "*")
        val city_name = Try(row(1).toString).getOrElse("null").replaceAll("\\.", "")
        val year = Try(row(2).toString.toInt).getOrElse(-1)
        val population = Try(row(3).toString.toDouble).getOrElse(0.0)
        val sex = PartOfPeople.strToChar(Try(row(4).toString).getOrElse("b"))
        City(country, city_name, year, population, sex)
      })
  }

  private def usefulRowsForBothSexes(data: RDD[City], year: Int = -1): RDD[City] = {
    val tmp_m = selectUsefulRows(data.filter(city => city.sex == 'm'), year)
    val tmp_f = selectUsefulRows(data.filter(city => city.sex == 'f'), year)
    tmp_m.union(tmp_f)
  }

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



