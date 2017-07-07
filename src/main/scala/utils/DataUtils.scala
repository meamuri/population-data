package utils

import dao.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

import scala.util.Try

/**
  * Модуль для работы с данными
  */
private object DataUtils {
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
    } else { // если нашли указанный год, берем за этот год, в противном случае самую свежую запись
      tmp.mapValues(cities => cities.find(city => city.year == year).getOrElse(cities.maxBy(_.year)))
    }
    res.map(pair => pair._2)
  }
}



