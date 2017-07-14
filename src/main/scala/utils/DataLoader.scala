package utils

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DataLoader(val basePath: String = "data", val sc: SparkContext) {

  private val fileBoth = basePath + "/unsd-citypopulation-year-both.csv"
  private val fileDiff = basePath + "/unsd-citypopulation-year-fm.csv"

  def loadDataWithBothSexes: RDD[Map[String, String]] = loadData(isBoth = true, sc)
  def loadDataWithDiffSexes: RDD[Map[String, String]] = loadData(isBoth = false, sc)

  /**
    * Функция, которой необходимо воспользоваться перед загрузкой данных
    * Функция осуществляет проверку, хранятся ли в директории, ассоциированной
    * с объектом-загрузчиком, необходимые для работы файлы
    * @return
    */
  def checkWorkFolder: Boolean = {
    val file_with_both_data = new File(fileBoth)
    val file_with_diff_data = new File(fileDiff)
    file_with_both_data.exists() && file_with_diff_data.exists()
  }

  private def loadData(isBoth: Boolean, sparkContext: SparkContext):
  RDD[Map[String, String]] = {
    val path =  if (isBoth) { fileBoth } else { fileDiff }
    val csv = sparkContext.textFile(path)
    val data = csv.map(line => line.split(",").map(elem => elem.trim))
    val header = data.first

    val result = data.map(splits => { header.zip(splits).toMap })
    result
  }

} // ... class DataLoader
