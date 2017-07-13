package application

import java.io.File

import com.typesafe.config.ConfigFactory
import factories.{MongoFactory, SparkFactory}
import utils.{DataLoader, MongoUtils, SparkUtils}

import scala.util.Try

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
    * Точка вхоа в приложение. Скрипт проверяет, есть ли нужные файлы,
    * и если они есть, запускает SparkContext и начинает работу с BigData:
    * обрабатывает CSV файлы, открывает соединение с MongoDB по порту 27017,
    * сохраняет результаты в запущенный экзмеляр
    * в базу данных "dsr"
    * и следующие коллекции, соответствующие частям задания:
    *   * top
    *   * ratio
    *   * population
    *   * millionaires
    * после завершения работы закрывается соедиение с Mongo и со SparkContext
    * @param args args(0) -- путь до папки, содержащий два файла:
    *             1. unsd-citypopulation-year-both.csv
    *             2. unsd-citypopulation-year-both.csv
    *             Если параметр пуст, пытаемся подставить './data'
    *             В любом случае проверяем наличие файлов в переданной папке
    *             args(1) -- год, за который берем данные по файлам.
    *             Если параметр не передан или равен -1, то смотрим самые
    *             новые записи. Если год указан, ищем записи именно этого года.
    *             В случае, если данных за этого год для какого-то города нет,
    *             все равно пытаемся смотреть самые свежие записи
    */
  def main(args: Array[String]) {
    val conf = ConfigFactory.parseFile(new File("resources/config/application.json"))
    println(conf.getInt("App.job.level"))

    val path = if (args.length == 0) { "data" } else { args(0) }
    val year = if (args.length < 2) { -1 } else { Try(args(1).toInt).getOrElse(-1) }

    val loader = new DataLoader(path)
    if (!loader.checkWorkFolder){
      println("По указанному пути нет необходимых для работы файлов!")
      return
    }

    val all_df = loader.loadDataWithBothSexes(SparkFactory.getSparkSession)
    val worker = new SparkUtils(all_df, year)
    val keeper = new MongoUtils

    val million_population_cities = worker.getCitiesWithMillionPopulation
    keeper.saveMillionaires(million_population_cities, MongoFactory.getMillionairesCollection)

    val population_by_countries = worker.getCountiesPopulation
    keeper.savePopulation(population_by_countries, MongoFactory.getPopulationCollection)

    val top5 = worker.getTop5cities
    keeper.saveTop5(top5, MongoFactory.getTopCollection)

    val ratio = worker.getRatio(loader.loadDataWithDiffSexes(SparkFactory.getSparkSession))
    keeper.saveRatio(ratio, MongoFactory.getRatioCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
