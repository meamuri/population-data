package application

import initial.{DataInit, SparkInit}
import utils.SparkUtils

/**
  * Точка входа в приложение
  *
  * Здесь будет условие
  */
object Job {
  def main(args: Array[String]) {
    val sc = SparkInit.getSparkContext
    SparkUtils.testSpark(sc)
  }
}
