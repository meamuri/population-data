package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

/**
  * Модуль для работы с данными
  */
object DataUtils {
  def chooseLastRecord(iterable: Iterable[List[Any]]): List[Any] = {
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

  def getAllCitiesRows(all_data: DataFrame): RDD[(Any, List[Any])] = {
    all_data.select("Country or Area", "City", "Year", "Value").rdd
      .map(x => (x(1), List(x(0), x(2), x(3))))
      .groupByKey()
      .mapValues(x => chooseLastRecord(x))
  }
}
