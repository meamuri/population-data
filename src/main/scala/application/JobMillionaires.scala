package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.Common
import services.Task
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
object JobMillionaires {

  def main(args: Array[String]) {
    val path = if (args.length == 0) { Resources.getDataPath } else { args(0) }
    val year = if (args.length < 2) { Resources.getYear } else { Try(args(1).toInt).getOrElse(-1) }

    if (!Common.checkWorkFolder(path)) {
      println("По указанному пути нет необходимых для работы файлов!")
      return
    }

    val loader = new DataLoader(path, year, SparkFactory.getSparkContext)
    val task = new Task(loader)

    task.calculateMillionaires()

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
