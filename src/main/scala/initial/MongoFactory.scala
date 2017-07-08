package initial

import com.mongodb.casbah.{MongoClient, MongoCollection, MongoDB}

/**
  * инициализация noSQL базы данных MongoDB
  */
object MongoFactory {
  private val SERVER = "localhost"
  private val PORT   = 27017
  private val DATABASE = "dsr_practice"
  private val COLLECTIONS = ("ratio", "top", "population", "millionaires")

  private val mongoClient = MongoClient(SERVER, PORT)
  private val db = mongoClient(DATABASE)

  def getMongoDatabase: MongoDB = { db }

  def getRatioCollection: MongoCollection = db(COLLECTIONS._1)
  def getTopCollection: MongoCollection = db(COLLECTIONS._2)
  def getPopulationCollection: MongoCollection = db(COLLECTIONS._3)
  def getMillionairesCollection: MongoCollection = db(COLLECTIONS._4)
}