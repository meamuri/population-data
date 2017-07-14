package utils

import java.io.File

import dao.{City, PartOfPeople}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Try

class DataLoader(val basePath: String = "data") {
  private val fileBoth = basePath + "/unsd-citypopulation-year-both.csv"
  private val fileDiff = basePath + "/unsd-citypopulation-year-fm.csv"

  def loadDataWithBothSexes(sc: SparkContext): RDD[City] = {
    val data = loadData(isBoth = true, sc)
    val cities = dataToCities(data)
    cities
  }

  def loadDataWithDiffSexes(sc: SparkContext): RDD[City] = {
    val data = loadData(isBoth = false, sc)
    val cities = dataToCities(data)
    cities
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

  private def selectUsefulRows(data: RDD[City], year: Int): RDD[City] = {
    val tmp = data.map(city => (city.name, city.copy()))
      .groupByKey()
    val res = if (year == -1) {
      tmp.mapValues(cities => cities.maxBy(_.year))
    } else { // если нашли указанный год, берем за этот год, в противном случае самую свежую запись
      tmp.mapValues(cities => cities.find(city => city.year == year).getOrElse(cities.maxBy(_.year)))
    }
    res.map(pair => pair._2.copy())
  }

  private def dataToCities(data: RDD[Map[String, String]]): RDD[City] = {
    data.map(row =>  City(
        country = Try(row.get("Country or Area").toString).getOrElse("null").replaceAll("\\.", ""),
        name = Try(row.get("City").toString).getOrElse("null").replaceAll("\\.", ""),
        year = Try(row.get("Year").toString.toInt).getOrElse(-1),
        population = Try(row.get("Value").toString.toDouble).getOrElse(0.0),
        sex = PartOfPeople.strToChar(Try(row.get("Sex").toString).getOrElse("b"))
      ))
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
