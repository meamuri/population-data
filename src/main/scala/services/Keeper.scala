package services

import com.mongodb.DBObject
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import dao.City
import org.apache.spark.rdd.RDD

/**
  *
  */
class Keeper (val generalInfo: String) {
  def saveMillionaires(data: RDD[(String, Int)], coll: MongoCollection): Unit = {
    coll.drop()
    data.collect()
      .foreach(x => coll.save(getDbObject(x._1, x._2, "millionaires_count")) )
  }

  def saveTop(data: RDD[(String, Iterable[City])], coll: MongoCollection): Unit = {
    coll.drop()
    val res = data
      .mapValues(cities => cities.map(c => Map("name" -> c.name, "population" -> c.population)))
    res.collect()
      .foreach(country => coll.save(getDbObject(country._1, country._2, "top")) )
  }


  def saveRatio(data: RDD[(String, Double)], coll: MongoCollection): Unit = {
    coll.drop()
    data.collect()
      .foreach(x => coll.save(getDbObject(x._1, x._2, "ratio")) )
  }

  def savePopulation(data: RDD[(String, Double)], coll: MongoCollection): Unit = {
    coll.drop()
    data.collect()
      .foreach(x => coll.save(getDbObject(x._1, x._2, "population")) )
  }

  private def getDbObject(country: String, info: Any, name: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += generalInfo -> country
    builder += name -> info
    builder.result
  }


}
