package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

/**
  * Модуль для работы с данными
  */
object DataUtils {
  private def chooseRecentlyRecord(iterable: Iterable[List[Any]]): List[Any] = {
    val l = iterable.toList

    var res = l.head
    var max_y =  0
    for (e <- l) {
      val curr_year = e(1) match {
        case Some(x: Int) => x // this extracts the value in a as an Int
        case _ => 0
      }
      if (curr_year > max_y) {
        res = e
        max_y = curr_year
      }
    }
    res
  }

  def getAllCities(all_data: DataFrame): RDD[(Any, List[Any])] = {
    all_data.select("Country or Area", "City", "Year", "Value").rdd
      .map(x => (x(1), List(x(0), x(2), x(3))))
      .groupByKey()
      .mapValues(x => chooseRecentlyRecord(x))
  }


  def getCitiesWithPopulationMoreThan(all_data: DataFrame, population: Int = 500): RDD[(Any, Any)] = {
    all_data.select("Country or Area", "City", "Year", "Value").rdd
      .filter(x => {
        val value = x(3) match {
          case Some(population: Double) => population // this extracts the value in a as an Int
          case _ => 0
        }
        value > population * 1000
      })
      .map(x => (x(1), List(x(0), x(2))))
      .groupByKey()
      .mapValues(x => chooseRecentlyRecord(x))
      .mapValues(x => x.head)
  }

  def getActualInfoByCountries(all_data: DataFrame): RDD[(Any, Iterable[List[Any]])] = {
    getAllCities(all_data).map(el => {
      (el._2.head, List(el._1, el._2(2))) // list(1) - название города, list(2) - население
    }).groupByKey()
  }

}
