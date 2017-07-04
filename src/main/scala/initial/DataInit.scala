package initial

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Загрузчик данных из файлов csv
  */
object DataInit {
  private val pathBoth = "data/unsd-citypopulation-year-both.csv"
  private val pathDiff = "data/unsd-citypopulation-year-fm.csv"

  def loadDataWithBothSexes(sparkSession: SparkSession): DataFrame = {
    loadData(isBoth = true, sparkSession)
  }

  def loadDataWithDiffSexes(sparkSession: SparkSession): DataFrame = {
    loadData(isBoth = false, sparkSession)
  }

  /**
    * Функция загрузки данных из csv файлов, хранящихся в каталоге проекта
    * @param isBoth логическая переменная, пределяющая,
    *               нужно ли загружать данные обоих полов, или нет
    * @return функция возвращает набор данных из соответствующего файла
    *         в виде DataFrame
    */
  private def loadData(isBoth: Boolean, sparkSession: SparkSession): DataFrame = {
    val path = if (isBoth) { pathBoth } else { pathDiff }
    val dataFrame = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(path)
    dataFrame
  }
}
