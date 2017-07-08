package utils

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

  def saveTop5(data: RDD[(String, City)]): Unit = {
    saveCountryToCitiesMap(data, collTop)
  }

  def saveMillionaires(data: RDD[(String, City)]): Unit = {
    saveCountryToCitiesMap(data, collMillionaires)
  }

  def saveRatio(data: RDD[(String, Double)]): Unit = {
    saveCountryToDoubleMap(data, collRatio)
  }

  def savePopultaion(data: RDD[(String, Double)]): Unit = {
    saveCountryToDoubleMap(data, collPopulation)
  }


  private def saveCountryToCitiesMap(data: RDD[(String, City)], c: MongoCollection): Unit = {
    val builder = MongoDBObject.newBuilder
    data.collect().foreach(x => { builder += x._1 -> x._2 } )
    val obj = builder.result()
    c.insert(obj)
  }

  private def saveCountryToDoubleMap(data: RDD[(String, Double)], c: MongoCollection): Unit = {
    val builder = MongoDBObject.newBuilder
    data.collect().foreach(x => { builder += x._1 -> x._2 } )
    val obj = builder.result()
    c.insert(obj)
  }
}
