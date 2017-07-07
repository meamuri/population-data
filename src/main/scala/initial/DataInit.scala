package initial

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Загрузчик данных из файлов csv
  */
class DataInit(val sparkSession: SparkSession, val basePath: String = "data") {
  private val fileBoth = basePath + "/unsd-citypopulation-year-both.csv"
  private val fileDiff = basePath + "/unsd-citypopulation-year-fm.csv"

  def loadDataWithBothSexes(): DataFrame = {
    loadData(isBoth = true, sparkSession)
  }

  def loadDataWithDiffSexes(): DataFrame = {
    loadData(isBoth = false, sparkSession)
  }

  def fileExists(path: String): Boolean = {
    new File(path).exists()
  }

  /**
    * Функция загрузки данных из csv файлов, хранящихся в каталоге проекта
    * @param isBoth логическая переменная, пределяющая,
    *               нужно ли загружать данные обоих полов, или нет
    * @return функция возвращает набор данных из соответствующего файла
    *         в виде DataFrame
    */
  private def loadData(isBoth: Boolean, sparkSession: SparkSession):
  DataFrame = {
    val path =  if (isBoth) { fileBoth } else { fileDiff }
    val dataFrame = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(path)
    dataFrame
  }
}
