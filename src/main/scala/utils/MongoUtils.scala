package utils

import com.mongodb.DBObject
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import dao.City
import org.apache.spark.rdd.RDD

/**
  * Некоторый функционал по работе с MongoDB
  */
class MongoUtils {
  /**
    * Функция сохраняет набор данных, представляющих собой топ 5 по численности
    * городов каждой страны, в базу MongoDB, в соответсвующий документ
    * @param data информацию по странам о топ 5 их городов по численности,
    *             представляющая собой RDD с наборами ключ-значение вида:
    *             (страна -> список городов)
    * @param collTop5 коллекция, куда требуется произвести сохранение
    */
  def saveTop5(data: RDD[(String, Iterable[City])], collTop5: MongoCollection): Unit = {
    collTop5.drop()
    data.collect()
      .foreach(x => collTop5.save(dbObjWithList(x._1, x._2)) )
  }

  /**
    * Функция сохраняет набор данных, представляющий собой информацию о количестве
    * городов-миллионников в станах, в базу MongoDB, в соотвествующий документ
    * @param data информация по странам о количестве городов-миллионников
    *             представляет собой RDD с наборами ключ-значение вида:
    *             (Страна -> Количество городов-миллионников)
    * @param collMillionaires коллекция, куда требуется произвести сохраниен
    */
  def saveMillionaires(data: RDD[(String, Int)], collMillionaires: MongoCollection): Unit = {
    collMillionaires.drop()
    data.collect()
      .foreach(x => collMillionaires.save(dbObjWithList(x._1, x._2)) )
  }

  /**
    * Функция сохраняет набор данных, представляющий собой информацию о соотношении
    * мужского и женского населений в станах, в базу MongoDB, в соотвествующий документ
    * @param data информация по странам о соотношении мужского и женского населения
    *             представляет собой RDD с наборами ключ-значение вида:
    *             (Страна -> Соотношение)
    * @param collRatio коллекция, куда требуется произвести сохраниен
    */
  def saveRatio(data: RDD[(String, Double)], collRatio: MongoCollection): Unit = {
    collRatio.drop()
    data.collect()
      .foreach(x => collRatio.save(dbObjWithDouble(x._1, x._2, "ratio")) )
  }

  /**
    * Функция сохраняет набор данных, представляющий собой информацию
    * о населении стран, в базу MongoDB, в соотвествующий документ
    * @param data информация по странам о количестве населения
    *             представляет собой RDD с наборами ключ-значение вида:
    *             (Страна -> Население)
    * @param collPopulation коллекция, куда требуется произвести сохраниен
    */
  def savePopulation(data: RDD[(String, Double)], collPopulation: MongoCollection): Unit = {
    collPopulation.drop()
    data.collect()
      .foreach(x => collPopulation.save(dbObjWithDouble(x._1, x._2, "population")) )
  }

  /**
    * Вспомогательная функция, создающая объект документа MongoDB
    * @param country Название страны
    * @param cities Список городов
    * @return Объект MongoDB для помещения в коллекцию
    */
  private def dbObjWithList(country: String, cities: Iterable[City]): DBObject = {
    val simple_cities = cities.map(c => Map("name" -> c.name, "population" -> c.population))
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += "cities" -> simple_cities
    builder.result
  }

  /**
    * Вспомогательная функция, создающая объект документа MongoDB
    * @param country Название страны
    * @param cnt Количество городов-миллионников
    * @return Объект MongoDB для помещения в коллекцию
    */
  private def dbObjWithList(country: String, cnt: Int): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += "city_million_more_population" -> cnt
    builder.result
  }

  /**
    * Вспомогательная функция, создающая объект документа mongoDB
    * @param country Название страны
    * @param info Численная информация
    * @param name Название, служащее ключом для info
    * @return Объект MongoDB для помещения в коллекцию
    */
  private def dbObjWithDouble(country: String, info: Double, name: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += name -> info
    builder.result
  }

} // ... class MongoUtils
