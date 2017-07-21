package helpers

import dao.Parameters
import factories.Resources


class Conf (val args: Array[String]){
  private val parser = new scopt.OptionParser[Parameters]("DSR") {
    head("data computing", "1.0")

    opt[String]('p', "path").action( (x,c) =>
      c.copy(path = x)).text("data is a text property")
    opt[Int]('y', "year").action( (x, c) =>
      c.copy(year = x) ).text("year is an integer property")
  }

  def getYear: Int = {
    parser.parse(args, Parameters()) match {
      case Some(config) => config.year
      case None => -1
    }
  }

  def getPath: String = {
    parser.parse(args, Parameters()) match {
      case Some(config) => config.path
      case None => Resources.getDataPath
    }
  }

}