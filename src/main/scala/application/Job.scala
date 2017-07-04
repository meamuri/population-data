package application

import initial.{DataInit, SparkInit}
import utils.{DataUtils, SparkUtils}

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
    val all_df = DataInit.loadDataWithBothSexes(SparkInit.getSparkSession)
    val df = DataUtils.getAllCitiesRows(all_df)
    df.take(5) foreach println
  }
}
