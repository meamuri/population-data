package application

import initial.{DataInit, SparkInit}

/**
  * Точка входа в приложение
  */
object Job {
  def main(args: Array[String]) {
    val sc = SparkInit.getSparkContext
  }
}
