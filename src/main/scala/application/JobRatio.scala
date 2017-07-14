package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.Common
import services.{Keeper, Miner}
import utils.DataLoader

import scala.util.Try

/**
  * Created by meamuri on 14.07.17.
  */
object JobRatio {
  def main(args: Array[String]) {
    val path = if (args.length == 0) { Resources.getDataPath } else { args(0) }
    val year = if (args.length < 2) { Resources.getYear } else { Try(args(1).toInt).getOrElse(-1) }

    if (!Common.checkWorkFolder(path)) {
      println("По указанному пути нет необходимых для работы файлов!")
      return
    }

    val loader = new DataLoader(path, year, SparkFactory.getSparkContext)
    val worker = new Miner(loader)
    val saver = new Keeper

    val res = worker.getRatio(Resources.isRationMaleToFemale)
    saver.saveRatio(res, MongoFactory.getRatioCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
