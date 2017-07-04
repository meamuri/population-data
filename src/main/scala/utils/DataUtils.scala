package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

/**
  * Модуль для работы с данными
  */
object DataUtils {
  def groupByCountries(all_data: DataFrame): RDD[(Any, List[Any])] = {

    val df = all_data
      .select("Country or Area", "City", "Year", "Value")
        .rdd
      .map(x => (
        x(0),
        List(x(1), x(2), x(3))
        )
      )
    df
  }

}
