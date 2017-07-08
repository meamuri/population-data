package utils

import com.mongodb.DBObject
import com.mongodb.casbah.{MongoCollection, MongoDB}
import com.mongodb.casbah.commons.MongoDBObject
import dao.City
import org.apache.spark.rdd.RDD

/**
  * Некоторый функционал по работе с MongoDB
  */
class MongoUtils(val mongoDB: MongoDB){
  val collTop: MongoCollection = mongoDB("top")
  val collMillionaires: MongoCollection = mongoDB("millionaires")
  val collRatio: MongoCollection = mongoDB("ratio")
  val collPopulation: MongoCollection = mongoDB("population")

  def saveTop5(data: RDD[(String, Iterable[City])]): Unit = {
    collTop.drop()
    data.collect()
      .foreach(x => collTop.save(dbObjWithList(x._1, x._2)) )
  }

  def saveMillionaires(data: RDD[(String, Iterable[City])]): Unit = {
    collMillionaires.drop()
    data.collect()
      .foreach(x => collMillionaires.save(dbObjWithList(x._1, x._2)) )
  }

  def saveRatio(data: RDD[(String, Double)]): Unit = {
    collRatio.drop()
    data.collect()
      .foreach(x => collRatio.save(dbObjWithDouble(x._1, x._2, "ratio")) )
  }

  def savePopulation(data: RDD[(String, Double)]): Unit = {
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

  private def dbObjWithDouble(country: String, info: Double, name: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += name -> info
    builder.result
  }
}
