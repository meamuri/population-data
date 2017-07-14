package utils

import com.mongodb.DBObject
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import dao.City
import org.apache.spark.rdd.RDD

/**
  * Некоторый функционал по работе с MongoDB
  */
class MongoUtils(private val genInfo: String) {

  def dbObjWithList(generalName: String, info: Iterable[Map[String, Any]],
                    infoName: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += genInfo -> generalName
    builder += infoName -> info
    builder.result
  }

  def dbObjWithInt(generalName: String, info: Int, infoName: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += genInfo -> generalName
    builder += infoName -> info
    builder.result
  }


  def dbObjWithDouble(generalName: String, info: Double, infoName: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += genInfo -> generalName
    builder += infoName -> info
    builder.result
  }

} // ... class MongoUtils
