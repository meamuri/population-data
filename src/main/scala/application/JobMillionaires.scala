package application

import factories.{MongoFactory, Resources, SparkFactory}
import utils.{DataLoader, MongoUtils, SparkUtils}

import scala.util.Try

/**
  *
  */
object JobMillionaires {
  def main(args: Array[String]) {
    val path = if (args.length == 0) { Resources.getDataPath } else { args(0) }
    val year = if (args.length < 2) { Resources.getYear } else { Try(args(1).toInt).getOrElse(-1) }

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

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
