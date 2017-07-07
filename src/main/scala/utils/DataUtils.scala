package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

import scala.util.Try

/**
  * Модуль для работы с данными
  */
object DataUtils {
  /**
    * Функция, позволяющая из набора записей об одном городе за разные годы
    * выбрать наиболее свежую информацию.
    * Так, каждая запись должна представлять собой список вида:
    * List("SMTHNG", "Year", "SMTHING")
    * где, второй элемент list(1) -- год актуальности информации.
    * По этому полю будет производиться сравнение
    * @param iterable набор всех записей о конкретном городе.
    * @return наиболее актуальная запись для города
    */
  private def chooseRecentlyRecord(iterable: Iterable[List[Any]]): List[Any] = {
    val l = iterable.toList

    // принимаем за наиболее актуальный год -- первую запись набора
    // при этом за год самой актуальной записи принимаем 0:
    // поскольку цикл пройдет по всем элементам, даже для первого элемента,
    // уже выбранного в качестве максимума, произойдет переприсваивание
    var res = l.head
    var max_y =  0
    for (e <- l) {
      // смотрим, на какой год рассматриваемые данные актуальны
      val curr_year = Try(e(1).toString.toInt).getOrElse(0)

      // если рассматриваемые данные более актуальные, чем уже принятые
      // за наиболее свежие, меняем хранимый в переменной результата набор
      if (curr_year > max_y) {
        res = e
        max_y = curr_year
      }
    }
    res // возвращаем набор в expression стиле
  }

  /**
    * Функция, позволяющая получить все города исходного набора (с информацией о населении
    * обоих полов)
    * @param all_data SQL-подобный объект DataFrame,
    *                 позволяющий выбрать актуальные записи и лишь необходимые поля:
    * @return rdd набор данных о всех городах в следующем формате:
    *         CityName -> ("Country", "Value")
    */
  def getAllCities(all_data: DataFrame): RDD[(Any, List[Any])] = {
    all_data.select("Country or Area", "City", "Year", "Value").rdd
      .map(x => (x(1), List(x(0), x(2), x(3))))
      .groupByKey()
      .mapValues(x => chooseRecentlyRecord(x)) // в x(1) обязательно Year!
      .mapValues(x => List(x.head, x(2))) // убираем ненужный теперь параметр Year
      // возващаемый набор представляет запой ключ CityName и значение (Country, Value)
  }

  /**
    * Получение всех городов с населением более заданного (параметр population * 1000).
    * На основе списка всех городов получается набор,
    * содержащий только самые актуальные записи о каждом городе
    * Затем происходит фильтрация по параметру "Население (Value)".
    * Затем мы формируем новые пары, где ключом служит страна, а значения -- информация о городе.
    * Поскольку страны (ключи) повторяются, группируем по странам.
    * @param all_data SQL-подобный объект DataFrame,
    *                 позволяющий выбрать актуальные записи и лишь необходимые поля:
    * @param population порог населения, в тысячах жителей.
    *                   Так, если необходимо получить города миллионники,
    *                   следует передать в качестве параметра 1000.
    *                   По умолчанию ищет все города населением более полумиллиона человек.
    * @return rdd набор данных городах, имеющих население более заданного
    *         Города сгруппированы по странам, и о каждом городе известно:
    *         ("CityName", "Value (население)")
    */
  def getCitiesWithPopulationMoreThan(all_data: DataFrame, population: Int = 500): RDD[(Any, List[Any])] = {
    all_data.select("Country or Area", "City", "Year", "Value")
      .rdd
      .filter(info => {
        val level = population * 1000
        val value = Try(info(3).toString.toDouble).getOrElse(0.0)
        value > level
      }) // сразу отбрасываем записи, в которых население меньше чем population*1000
      .map(x => (x(1), List(x(0), x(2), x(3))))
      .groupByKey()
      .mapValues(x => chooseRecentlyRecord(x))
        // возможно, наиболее удачное расположение функции фильтрации:
        // города до миллиона жителей могут иметь записи за несколько лет,
        // а мы все равно проверяли для них предикат функции filter
      .mapValues(x => List(x.head, x(2))) // в итоговом наборе в ключах осталяем лишь страну и население
  }

  /**
    * Возможность получить города. сгруппированные по странам. Инфа о каждом городе - наиболее актуальная
    * @param all_data SQL-подобный объект DataFrame,
    *                 позволяющий выбрать актуальные записи и лишь необходимые поля:
    * @return Набор отображений, ставящий каждому ключу в виде имени страны
    *         Значение в виде списка, каждый элемент которого хранит запись вида
    *         (CityName, Value/Population) тоже список, т.е. обращение как x(0), x(1) соответственно
    */
  def getActualInfoByCountries(all_data: DataFrame): RDD[(Any, Iterable[List[Any]])] = {
    getAllCities(all_data).map(el => {
      (el._2.head, List(el._1, el._2(2))) // list(0) - название города, list(1) - население
    }).groupByKey()
  }

  /**
    * Функция считает, какое население каждой страны по данным о населении городов
    * @param all_data SQL-подобный объект DataFrame,
    *                 позволяющий выбрать актуальные записи и лишь необходимые поля:
    * @return возвращает набор отображений
    * (название страны => население страны )
    */
  def getPopulationOfCountries(all_data: DataFrame): RDD[(Any, Any)] = {
    val countriesWithFull = getAllCities(all_data).map(el => {
      (el._2.head, el._2(1)) // list(1) - название города, list(2) - население
    }).groupByKey()

    countriesWithFull.mapValues(list => {
      var sum = 0.0
      for (el <- list) {
        val people_in_city = Try(el.toString.toDouble).getOrElse(0.0)
        sum += people_in_city
      }
      sum
    })
  } // ... def getPopulationOfCountries

  def getTopNByCountries(all_data: DataFrame, top: Int): RDD[(Any, Iterable[List[Any]])] = {
    val info = getAllCities(all_data).map(el => {
      (el._2.head, List(el._1, el._2(2))) // list(0) - название города, list(1) - население
    }).groupByKey()

    info.mapValues(list => {
      val res = list.toList
      res.sortBy(k => Try(k(1).toString.toDouble).getOrElse(0.0)).take(top)
    })
  }

} // ...obj DataUtils
