package application

import initial.{DataInit, SparkInit}
import utils.{SparkUtils}

/**
  * Точка входа в приложение
  *
  * Необходимо рассчитать:
  *  * население стран
  *  * для каждой страны:
  *    * города миллионники
  *    * топ 5 самых крупных
  *    * соотношение мужчин/женщин
  *
  *  результат сохранить в MongoDb
  *
  */
object Job {
  def main(args: Array[String]) {
    val all_df = DataInit.loadDataWithBothSexes(SparkInit.getSparkSession)
    val million_population_cities = SparkUtils.getCitiesWithMillionPopulation(all_df)
    println(million_population_cities.count())
    val res = million_population_cities.collect()
    println(res.length)
    for (el <- res.take(5)) println(el)
  }
}
