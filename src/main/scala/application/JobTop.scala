package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.Common
import services.{Keeper, Miner}
import utils.DataLoader

import scala.util.Try

object JobTop {
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

    val dataFrame = loader.loadData(fileBoth, SparkFactory.getSparkSession)
    val cities = loader.selectBothRows(dataFrame, year)

    val worker = new Miner
    val res = worker.countriesWithTopN(cities, Resources.getTop)

    val saver = new Keeper("country")
    saver.saveTop(res, MongoFactory.getTopCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
