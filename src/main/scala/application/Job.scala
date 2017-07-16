package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.Common
import services.{Keeper, Miner}
import utils.DataLoader

import scala.util.Try

/**
  *
  */
object Job {
  def main(args: Array[String]) {
    val path = if (args.length == 0) { Resources.getDataPath } else { args(0) }
    val year = if (args.length < 2) { Resources.getYear } else { Try(args(1).toInt).getOrElse(-1) }

    val fileBoth = path + Resources.getBothFilename
    val fileDiff = path + Resources.getDiffFilename
    if (!Common.folderContainFiles(Iterable(fileBoth, fileDiff))){
      println(Resources.getIncorrectPathMsg)
      return
    }

    val loader = new DataLoader
    val cities = loader.noSqlDiff(fileDiff, SparkFactory.getSparkContext, year)
    val second_set = loader.noSqlBoth(fileBoth, SparkFactory.getSparkContext, year)

    val worker = new Miner
    val res = worker.getRatio(cities)
    val res_population = worker.countriesPopulation(second_set)

    val saver = new Keeper("country")
    saver.saveRatio(res, MongoFactory.getRatioCollection)
    saver.savePopulation(res_population, MongoFactory.getPopulationCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
