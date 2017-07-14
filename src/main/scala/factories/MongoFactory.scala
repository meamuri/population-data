package factories

import com.mongodb.casbah.{MongoClient, MongoCollection}

/**
  * инициализация noSQL базы данных MongoDB
  */
object MongoFactory {
  private val SERVER = Resources.getDbServer
  private val PORT   = Resources.getDbPort
  private val DATABASE = Resources.getDbName

  private val mongoClient = MongoClient(SERVER, PORT)
  private val db = mongoClient(DATABASE)

  def getRatioCollection: MongoCollection = db(Resources.getCollRatio)
  def getTopCollection: MongoCollection = db(Resources.getCollTop)
  def getPopulationCollection: MongoCollection = db(Resources.getCollPopultaion)
  def getMillionairesCollection: MongoCollection = db(Resources.getCollMillionaires)

  def closeConnection(): Unit = {
    mongoClient.close()
  }
}
