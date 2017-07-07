package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Работа со Spark
  */
object SparkUtils {
  def getCitiesWithMillionPopulation(allData: DataFrame): RDD[(Any, List[Any])] = {
    DataUtils.getCitiesWithPopulationMoreThan(all_data = allData, 1000)
  }

  def getCountiesPopulation(allData: DataFrame): RDD[(Any, Any)] = {
    DataUtils.getPopulationOfCountries(allData)
  }

  def getTop5_cities(allData: DataFrame): RDD[(Any, Iterable[List[Any]])] = {
    DataUtils.getTopNByCountries(allData, 5)
  }
}
