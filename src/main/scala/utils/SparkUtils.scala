package utils

import dao.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Работа со Spark
  */
class SparkUtils(all_data: DataFrame, year: Int) {
  private val cities = DataUtils.getCities(all_data, year)

  /**
    * Функция позволяет получить данные,
    * сколько городов-миллионников в странах.
    * @return Каждой стране будет поставлено в соответствие
    *         число городов-миллионников в ней.
    *         RDD набор отображений (строка, целое число)
    */
  def getCitiesWithMillionPopulation: RDD[(String, Int)] = {
    val filtered_rows = filterByPopulation(cities, 1000)
    val res = groupCitiesByCountries(filtered_rows)
    res.mapValues(it => it.size)
  }

  /**
    * Функция позволяет получить данные
    * о количестве населения каждой страны
    * @return Каждой стране будет поставлено в соотвествие число:
    *         количество жителей в этой стране.
    *         RDD набор отображений (строка, вещественное число)
    */
  def getCountiesPopulation: RDD[(String, Double)] = {
    countriesPopulation(cities)
  }

  /**
    * Функция позволяет получить данные о городах,
    * входящих в топ-5 по населению для каждой страны
    * @return Каждой стране будет поставлен в соотвествие некоторый список
    *         из городов, входящих в топ
    */
  def getTop5cities: RDD[(String, Iterable[City])] = {
    countriesWithTopN(cities, 5)
  }

  /**
    * Функция позволяет получить данные о том,
    * каково соотношение мужского и женского населения в странах
    * @param data_by_sexes Информация о всех городах, где каждая запись
    *                      соответствует либо количество мужского населения,
    *                      либо количеству женского, но не населению обоих полов сразу
    * @return Каждой стране будет поставлено в соотвествие число:
    *         отношение количества мужского населения к женскому: m / f.
    *         RDD набор отображений (строка, вещественное число)
    */
  def getRatio(data_by_sexes: DataFrame): RDD[(String, Double)] = {
    val cities = DataUtils.getCitiesBothSexes(data_by_sexes, year)
    cities.map(city => (city.country, city))
      .groupByKey()
      .mapValues(cities_of_country => {
        var male = 0.0
        var female = 0.0
        for (c <- cities_of_country) {
          if (c.sex == 'm') {
            male = male + c.population
          } else{
            female = female + c.population}
        }
        male / female
      })
  }

  /**
    * Вспомогательная функция, группирующая уникальные записи о городах по странам
    * @param cities Записи о городах
    * @return Набор отображений (страна -> список ее городов)
    */
  private def groupCitiesByCountries(cities: RDD[City]): RDD[(String, Iterable[City])] = {
    cities.map(city => (city.country, city)).groupByKey()
  }

  /**
    * Отбирает города с населением выше заданного
    * @param cities Записи о городах
    * @param level Какой уровень населения необходимо отобрать.
    *              Исчисляется в тысячах населения, поэтому чтобы, например,
    *              выбрать города с населением более миллиона человек (1000 х 1000)
    *              необходимо передать в качестве параметра 1000.
    *              По умолчанию отбирает города с населением более 100 000 человек.
    * @return Набор городов с количеством населением выше заданного
    */
  private def filterByPopulation(cities: RDD[City], level: Int = 100): RDD[City] = {
    cities.filter(city => city.population > level * 1000)
  }

  /**
    * Функция из предложенного набора городов получает
    * отображение ( Страна -> численность населения)
    * @param cities список всех городов, которые будут сгруппированы
    *               по странам, и их численности будут просуммированы для каждой страны
    * @return Набор отображений (страна -> численность населения)
    */
  private def countriesPopulation(cities: RDD[City]): RDD[(String, Double)] = {
    cities.map(city => (city.country, city.population))
      .groupByKey()
      .mapValues(all_populations => all_populations.sum)
  }

  /**
    * Отбирает для каждой страны N городов в порядке убывания численности населения:
    * другими словами формирует топ N городов
    * @param cities города, из которых следует производить выборку
    * @param n количество городов, которые необходимо отсортировать и выбрать для каждой страны.
    *          По умолчанию находит город с наибольшей населенностью
    * @return для каждой страны находит топ-n городов по населению
    */
  private def countriesWithTopN(cities: RDD[City], n: Int = 1): RDD[(String, Iterable[City])] = {
    val countries = cities.map(city => (city.country, city)).groupByKey()
    countries.mapValues(cities_of_country => {
      cities_of_country.toList.sortBy(city => city.population).take(n)
    })
  }

} // ... class SparkUtils
