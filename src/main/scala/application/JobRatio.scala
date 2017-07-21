package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.{Common, Conf}
import services.{Keeper, Miner}
import utils.DataLoader

/**
  *
  */
object JobRatio {
  def main(args: Array[String]) {
    val conf = new Conf(args)
    val path = conf.getPath
    val year = conf.getYear

    val fileBoth = path + Resources.getBothFilename
    val fileDiff = path + Resources.getDiffFilename
    if (!Common.folderContainFiles(Iterable(fileBoth, fileDiff))){
      println(Resources.getIncorrectPathMsg)
      return
    }

    val loader = new DataLoader

    val dataFrame = loader.loadData(fileDiff, SparkFactory.getSparkSession)
    val cities = loader.selectDiffRows(dataFrame, year)

    val worker = new Miner
    val res = worker.getRatio(cities)

    val saver = new Keeper("country")
    saver.saveRatio(res, MongoFactory.getRatioCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
