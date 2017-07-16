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

    val cities = loader.noSqlLoading(fileBoth, SparkFactory.getSparkContext, year)

    cities.take(5).foreach(p => println(p))
    println(cities.count())

    val worker = new Miner
    val res = worker.countriesWithTopN(cities, 1)

    res.take(10).foreach(row => println(row))

//    val saver = new Keeper("country")
//    saver.saveTop(res, MongoFactory.getTopCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
