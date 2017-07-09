package utils

import com.mongodb.DBObject
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import dao.City
import org.apache.spark.rdd.RDD

/**
  * Некоторый функционал по работе с MongoDB
  */
object MongoUtils{
  def saveTop5(data: RDD[(String, Iterable[City])], collTop5: MongoCollection): Unit = {
    collTop5.drop()
    data.collect()
      .foreach(x => collTop5.save(dbObjWithList(x._1, x._2)) )
  }

  def saveMillionaires(data: RDD[(String, Int)], collMillionaires: MongoCollection): Unit = {
    collMillionaires.drop()
    data.collect()
      .foreach(x => collMillionaires.save(dbObjWithList(x._1, x._2)) )
  }

  def saveRatio(data: RDD[(String, Double)], collRatio: MongoCollection): Unit = {
    collRatio.drop()
    data.collect()
      .foreach(x => collRatio.save(dbObjWithDouble(x._1, x._2, "ratio")) )
  }

  def savePopulation(data: RDD[(String, Double)], collPopulation: MongoCollection): Unit = {
    collPopulation.drop()
    data.collect()
      .foreach(x => collPopulation.save(dbObjWithDouble(x._1, x._2, "population")) )
  }

  private def dbObjWithList(country: String, cities: Iterable[City]): DBObject = {
    val simple_cities = cities.map(c => Map("name" -> c.name, "population" -> c.population))
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += "cities" -> simple_cities
    builder.result
  }

  private def dbObjWithList(country: String, cnt: Int): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += "city_million_more_population" -> cnt
    builder.result
  }

  private def dbObjWithDouble(country: String, info: Double, name: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += name -> info
    builder.result
  }
}
