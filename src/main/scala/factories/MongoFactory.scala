package factories

import com.mongodb.casbah.{MongoClient, MongoCollection}

/**
  * инициализация noSQL базы данных MongoDB
  */
object MongoFactory {
  private val SERVER = "localhost"
  private val PORT   = 27017
  private val DATABASE = "dsr"
  private val COLLECTIONS = ("ratio", "top", "population", "millionaires")

  private val mongoClient = MongoClient(SERVER, PORT)
  private val db = mongoClient(DATABASE)

  def getRatioCollection: MongoCollection = db(COLLECTIONS._1)
  def getTopCollection: MongoCollection = db(COLLECTIONS._2)
  def getPopulationCollection: MongoCollection = db(COLLECTIONS._3)
  def getMillionairesCollection: MongoCollection = db(COLLECTIONS._4)

  def closeConnection(): Unit = {
    mongoClient.close()
  }
}
