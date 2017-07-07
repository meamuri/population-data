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
    println("args.len & args: ", args.length, args.toString)
    val all_df = DataInit.loadDataWithBothSexes(SparkInit.getSparkSession)

    val million_population_cities = SparkUtils.getCitiesWithMillionPopulation(all_df)
    println("Количество городов миллионников: " + million_population_cities.count())
    for (el <- million_population_cities.take(5)) println("Пример городов миллионников: " + el)

    val population_by_countries = SparkUtils.getCountiesPopulation(all_df)
    println("Количество стран: " + population_by_countries.count())
    for (el <- population_by_countries.take(5)) println("Население страны: " + el)
  }
}
