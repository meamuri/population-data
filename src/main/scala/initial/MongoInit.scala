package initial

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject

/**
  * инициализация noSQL базы данных MongoDB
  */
object MongoInit {

  private val mongoClient = MongoClient("casbah_text")

  def getMongoClient: MongoClient = { mongoClient }
}
