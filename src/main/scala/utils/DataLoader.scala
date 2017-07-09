package utils

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Загрузчик данных из файлов csv
  * Перед загрузкой данных, необходимо проверить наличие файлов
  * функцией checkWorkFolder.
  * В случае возвращения функцией неудачного результата,
  * попытки загрузить данные приведут к исключительным ситуациям
  */
class DataLoader(val basePath: String = "data") {
  private val fileBoth = basePath + "/unsd-citypopulation-year-both.csv"
  private val fileDiff = basePath + "/unsd-citypopulation-year-fm.csv"

  /**
    * Загружает данные с информацией о населении обоих полов.
    *
    * Обертка над функцией loadData
    * позволяющая абстрагироваться от передачи ей логического параметра,
    * определяющего, какой файл загружать
    * @param sparkSession сессия работы библиотеки Spark, с помощью которой
    *                     будет произведено считывание данных CSV файла
    * @return функция возвращает набор данных из соответствующего файла
    *         в виде DataFrame
    */
  def loadDataWithBothSexes(sparkSession: SparkSession): DataFrame = {
    loadData(isBoth = true, sparkSession)
  }

  /**
    * Загружает данные с информацией о населении,
    * где данные о разных полах представлены разными строками данных.
    *
    * Обертка над функцией loadData
    * позволяющая абстрагироваться от передачи ей логического параметра,
    * определяющего, какой файл загружать
    * @param sparkSession сессия работы библиотеки Spark, с помощью которой
    *                     будет произведено считывание данных CSV файла
    * @return функция возвращает набор данных из соответствующего файла
    *         в виде DataFrame
    */
  def loadDataWithDiffSexes(sparkSession: SparkSession): DataFrame = {
    loadData(isBoth = false, sparkSession)
  }

  /**
    * Функция, которой необходимо воспользоваться перед загрузкой данных
    * Функция осуществляет проверку, хранятся ли в директории, ассоциированной
    * с объектом-загрузчиком, необходимые для работы файлы
    * @return
    */
  def checkWorkFolder(): Boolean = {
    val file_with_both_data = new File(fileBoth)
    val file_with_diff_data = new File(fileDiff)
    file_with_both_data.exists() && file_with_diff_data.exists()
  }

  /**
    * Функция загрузки данных из csv файлов, хранящихся в каталоге проекта
    * @param isBoth логическая переменная, пределяющая,
    *               нужно ли загружать данные обоих полов, или нет
    * @param sparkSession сессия работы библиотеки Spark, с помощью которой
    *                     будет произведено считывание данных CSV файла
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
