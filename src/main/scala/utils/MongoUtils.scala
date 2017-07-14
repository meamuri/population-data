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

  def dbObjWithList(country: String, cities: Iterable[City]): DBObject = {
    val simple_cities = cities.map(c => Map("name" -> c.name, "population" -> c.population))
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += "cities" -> simple_cities
    builder.result
  }

  def dbObjWithList(country: String, cnt: Int): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += "city_million_more_population" -> cnt
    builder.result
  }


  def dbObjWithDouble(country: String, info: Double, name: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "country" -> country
    builder += name -> info
    builder.result
  }

} // ... class MongoUtils
