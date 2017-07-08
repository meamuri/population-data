package application

import initial.{DataInit, MongoInit, SparkInit}
import utils.{MongoUtils, SparkUtils}

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
    val db_manager = new MongoUtils(MongoInit.getMongoDatabase)

    val all_df = loader.loadDataWithBothSexes()
    val worker = new SparkUtils(all_df)

    val million_population_cities = worker.getCitiesWithMillionPopulation
    db_manager.saveMillionaires(million_population_cities)
    println("Количество городов миллионников: " + million_population_cities.count())
    for (el <- million_population_cities.take(5)) println("Пример городов миллионников: " + el)

    val population_by_countries = worker.getCountiesPopulation
    db_manager.savePopulation(population_by_countries)
    println("Количество стран с миллионниками: " + population_by_countries.count())
    for (el <- population_by_countries.take(5)) println("Население страны: " + el)

    val top5 = worker.getTop5_cities
    db_manager.saveTop5(top5)
    println("Количество стран с топом: " + top5.count())
    for (el <- top5.take(5)) println("Топ 5 по 5: " + el)

    val ratio = worker.getRatio(loader.loadDataWithDiffSexes())
    db_manager.saveRatio(ratio)
    println("Количество стран с рейтингом: " + ratio.count())
    for (el <- ratio.take(5)) println("Соотношение: " + el)

    val db = MongoInit.getMongoDatabase
    val coll = db("ratio")
    for (c <- coll.take(15)) println(c)
  }
}
