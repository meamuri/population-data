package utils

import dao.{City, PartOfPeople}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try


class DataLoader {
  def loadData(path: String, session: SparkSession):  DataFrame = {
    val dataFrame = session.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(path)
    dataFrame
  }

  def selectBothRows(data: DataFrame, year: Int): RDD[City] = {
    val rows = selectUsefulColumns(data)
    selectUsefulRows(rows, year)
  }

  def selectDiffRows(data: DataFrame, year: Int): RDD[City] = {
    val rows = selectUsefulColumns(data)
    selectUsefulRows(rows.filter(city => city.sex == 'm'), year)
      .union(selectUsefulRows(rows.filter(city => city.sex == 'f'), year))
  }

  private def selectUsefulColumns(all_data: DataFrame): RDD[City] = {
    all_data.select("Country or Area", "City", "Year", "Value", "Sex").rdd
      .map(row => {
        val country = Try(row(0).toString).getOrElse("null").replaceAll("\\.", " ")
        val city_name = Try(row(1).toString).getOrElse("null").replaceAll("\\.", " ")
        val year = Try(row(2).toString.toInt).getOrElse(-1)
        val population = Try(row(3).toString.toDouble).getOrElse(0.0)
        val sex = PartOfPeople.strToChar(Try(row(4).toString).getOrElse("b"))
        City(country, city_name, year, population, sex)
      })
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


}
