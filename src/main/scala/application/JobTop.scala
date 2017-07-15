package application

import factories.{MongoFactory, Resources, SparkFactory}
import helpers.Common
import utils.{DataLoader, MongoUtils, SparkUtils}

import scala.util.Try

object JobTop {
  def main(args: Array[String]) {
    val path = if (args.length == 0) { Resources.getDataPath } else { args(0) }
    val year = if (args.length < 2) { Resources.getYear } else { Try(args(1).toInt).getOrElse(-1) }

    val files = List(path + Resources.getBothFilename, path + Resources.getDiffFilename)
    if (!Common.folderContainFiles(files)){
      println("По указанному пути нет необходимых для работы файлов!")
      return
    }

    val loader = new DataLoader(path)

    val all_df = loader.loadDataWithBothSexes(SparkFactory.getSparkSession)
    val worker = new SparkUtils(all_df, year)
    val keeper = new MongoUtils

    val top5 = worker.getTop5cities
    keeper.saveTop5(top5, MongoFactory.getTopCollection)

    MongoFactory.closeConnection()
    SparkFactory.closeSession()
  }
}
