package application

import initial.DataInit

/**
  * Точка входа в приложение
  */
object Job {
  def main(args: Array[String]) {
    DataInit.initDataBase()
    DataInit.initCsv()
    DataInit.initSpark()
  }
}
