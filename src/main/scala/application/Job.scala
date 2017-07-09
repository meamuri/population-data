package application

import initial.{DataInit, MongoFactory, SparkInit}
import utils.{MongoUtils, SparkUtils}

/**
  * Точка входа в приложение
  *
  * Необходимо рассчитать:
  *  * население стран
  *  * для каждой страны:
  *    * количество городов миллионников
  *    * топ 5 самых крупных
  *    * соотношение мужчин/женщин
  *
  *  результат сохранить в MongoDb
  *
  */
object Job {
  /**
    *
    * @param args args(1) путь до папки, содержащий два файла:
    *             1. unsd-citypopulation-year-both.csv
    *             2. unsd-citypopulation-year-both.csv
    *             Если параметр пуст, пытаемся подставить './data'
    *             В любом случае проверяем наличие файлов в переданной папке
    */
  def main(args: Array[String]) {
    val path = if (args.length == 0) { "data" } else { args(0) }

    val loader = new DataInit(SparkInit.getSparkSession, path)
    val all_df = loader.loadDataWithBothSexes()
    val worker = new SparkUtils(all_df)

    val million_population_cities = worker.getCitiesWithMillionPopulation
    MongoUtils.saveMillionaires(million_population_cities, MongoFactory.getMillionairesCollection)

    val population_by_countries = worker.getCountiesPopulation
    MongoUtils.savePopulation(population_by_countries, MongoFactory.getPopulationCollection)

    val top5 = worker.getTop5_cities
    MongoUtils.saveTop5(top5, MongoFactory.getTopCollection)

    val ratio = worker.getRatio(loader.loadDataWithDiffSexes())
    MongoUtils.saveRatio(ratio, MongoFactory.getRatioCollection)

    MongoFactory.closeConnection()
    SparkInit.getSparkSession.close()
  }
}
