package services

import com.mongodb.casbah.MongoCollection
import dao.City
import org.apache.spark.rdd.RDD
import utils.MongoUtils

/**
  *
  */
class Keeper {
  val utils = new MongoUtils

  def saveTop(data: RDD[(String, Iterable[City])], collTop5: MongoCollection): Unit = {
    collTop5.drop()
    data.collect()
      .foreach(x => collTop5.save(utils.dbObjWithList(x._1, x._2)) )
  }

  def saveMillionaires(data: RDD[(String, Int)], collMillionaires: MongoCollection): Unit = {
    collMillionaires.drop()
    data.collect()
      .foreach(x => collMillionaires.save(utils.dbObjWithList(x._1, x._2)) )
  }

  def saveRatio(data: RDD[(String, Double)], collRatio: MongoCollection): Unit = {
    collRatio.drop()
    data.collect()
      .foreach(x => collRatio.save(utils.dbObjWithDouble(x._1, x._2, "ratio")) )
  }

  def savePopulation(data: RDD[(String, Double)], collPopulation: MongoCollection): Unit = {
    collPopulation.drop()
    data.collect()
      .foreach(x => collPopulation.save(utils.dbObjWithDouble(x._1, x._2, "population")) )
  }


}
