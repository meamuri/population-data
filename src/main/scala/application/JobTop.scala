package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.{Common, Conf}
import services.{Keeper, Miner}
import utils.DataLoader

object JobTop {
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
