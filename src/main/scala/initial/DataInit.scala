package initial

import java.io.File

import com.github.tototoshi.csv.CSVReader

/**
  * Загрузчик данных из файлов csv
  */
object DataInit {
  private val pathBoth = "data/unsd-citypopulation-year-both.csv"
  private val pathDiff = "data/unsd-citypopulation-year-fm.csv"

  /**
    * Функция загрузки данных из csv файлов, хранящихся в каталоге проекта
    * @param isBoth логическая переменная, пределяющая,
    *               нужно ли загружать данные обоих полов, или нет
    * @return функция возвращает набор данных из соответствующего файла
    *         в виде коллекции строк
    */
  def loadData(isBoth: Boolean): List[Map[String, String]] = {
    val path = if (isBoth) { pathBoth } else { pathDiff }
    val reader = CSVReader.open(new File(path))
    val info = reader.allWithHeaders()
    reader.close()
    info
  }
}
