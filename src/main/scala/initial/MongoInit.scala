package initial

import com.mongodb.casbah.{MongoClient, MongoCollection, MongoDB}

/**
  * инициализация noSQL базы данных MongoDB
  */
object MongoInit {
  private val mongoClient = MongoClient("test")
  private val db = mongoClient("test")

  def getMongoDatabase: MongoDB = { db }
  def getMongoClient: MongoClient = { mongoClient }
}
