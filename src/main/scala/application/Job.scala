package application

import initial.{DataInit, SparkInit}
import utils.SparkUtils

/**
  * Точка входа в приложение
  *
  * Необходимо рассчитать:
  *  * население стран
  *  * для каждой страны:
  *    * города миллионники
  *    * топ 5 самых крупных
  *    * соотношение мужчин/женщин
  *
  *  результат сохранить в MongoDb
  *
  */
object Job {
  def main(args: Array[String]) {
    val df = DataInit.loadDataWithBothSexes(SparkInit.getSparkSession)
    val new_df = df.select("value")
    println("\n\n")
    new_df.rdd.take(5).foreach(println)
  }
}
