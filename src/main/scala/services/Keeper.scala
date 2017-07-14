package services

import com.mongodb.casbah.MongoCollection
import dao.City
import org.apache.spark.rdd.RDD
import utils.MongoUtils

/**
  *
  */
class Keeper {
  val utils = new MongoUtils("Country")

  def saveTop(data: RDD[(String, Iterable[City])], coll: MongoCollection): Unit = {
    coll.drop()
    val res = data
      .mapValues(cities => cities.map(c => Map("name" -> c.name, "population" -> c.population)))
    res.collect()
      .foreach(country => coll.save(utils.dbObjWithList(country._1, country._2, "top")) )
  }

  def saveMillionaires(data: RDD[(String, Int)], coll: MongoCollection): Unit = {
    coll.drop()
    data.collect()
      .foreach(x => coll.save(utils.dbObjWithInt(x._1, x._2, "millionaires")) )
  }

  def saveRatio(data: RDD[(String, Double)], coll: MongoCollection): Unit = {
    coll.drop()
    data.collect()
      .foreach(x => coll.save(utils.dbObjWithDouble(x._1, x._2, "ratio")) )
  }

  def savePopulation(data: RDD[(String, Double)], coll: MongoCollection): Unit = {
    coll.drop()
    data.collect()
      .foreach(x => coll.save(utils.dbObjWithDouble(x._1, x._2, "population")) )
  }


}
