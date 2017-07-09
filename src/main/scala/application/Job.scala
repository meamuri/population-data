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
    val db = MongoFactory.getMongoDatabase
    val db_manager = new MongoUtils(db)
    val all_df = loader.loadDataWithBothSexes()
    val worker = new SparkUtils(all_df)

    val million_population_cities = worker.getCitiesWithMillionPopulation
    db_manager.saveMillionaires(million_population_cities)

    val population_by_countries = worker.getCountiesPopulation
    db_manager.savePopulation(population_by_countries)

    val top5 = worker.getTop5_cities
    db_manager.saveTop5(top5)

    val ratio = worker.getRatio(loader.loadDataWithDiffSexes())
    db_manager.saveRatio(ratio)

    println("\nНасеелние:")
    val collPop = db("population")
    for (c <- collPop.take(7)) println(c)

    println("\nТопы-5:")
    val collTop = db("top")
    for (c <- collTop.take(7)) println(c)

    println("\nКоличество городов с населением более миллиона:")
    val collMill = db("millionaires")
    for (c <- collMill.take(7)) println(c)

    println("\nСоотношение м/ж:")
    val coll = db("ratio")
    for (c <- coll.take(7)) println(c)

    SparkInit.getSparkSession.close()
  }
}
