package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.Common
import services.{Keeper, Miner}
import utils.{DataLoader, MongoUtils, SparkUtils}

import scala.util.Try

/**
  *
  */
object JobMillionaires {
  def main(args: Array[String]) {
    val path = if (args.length == 0) { Resources.getDataPath } else { args(0) }
    val year = if (args.length < 2) { Resources.getYear } else { Try(args(1).toInt).getOrElse(-1) }

    val files = List(path + Resources.getBothFilename, path + Resources.getDiffFilename)
    if (!Common.folderContainFiles(files)){
      println("По указанному пути нет необходимых для работы файлов!")
      return
    }

    val loader = new DataLoader

    val dataFrame = loader.loadData(files.head, SparkFactory.getSparkSession)
    val cities = loader.selectBothRows(dataFrame, year)

    //    val million_population_cities = worker.getCitiesWithMillionPopulation


    val saver = new Keeper
    //    keeper.saveMillionaires(million_population_cities, MongoFactory.getMillionairesCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
