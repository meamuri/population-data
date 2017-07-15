package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.Common
import services.{Keeper, Miner}
import utils.DataLoader

import scala.util.Try

/**
  *
  */
object JobRatio {
  def main(args: Array[String]) {
    val path = if (args.length == 0) { Resources.getDataPath } else { args(0) }
    val year = if (args.length < 2) { Resources.getYear } else { Try(args(1).toInt).getOrElse(-1) }

    val files = List(path + Resources.getBothFilename, path + Resources.getDiffFilename)
    if (!Common.folderContainFiles(files)){
      println(Resources.getIncorrectPathMsg)
      return
    }

    val loader = new DataLoader

    val dataFrame = loader.loadData(files(1), SparkFactory.getSparkSession)
    val cities = loader.selectDiffRows(dataFrame, year)

    val worker = new Miner
    val res = worker.getRatio(cities)

    val saver = new Keeper("country")
    saver.saveRatio(res, MongoFactory.getRatioCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
