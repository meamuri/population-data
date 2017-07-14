package factories

import com.typesafe.config.ConfigFactory

/**
  * Все данные приложения, считанные из файла конфигурации.
  */
object Resources {
  private val config = ConfigFactory.load

  def getDataPath: String = config.getString("data")
  def getYear: Int = config.getInt("year")

  def getLevel: Int = config.getInt("job.level")
  def getTop: Int = config.getInt("job.top")
  def isRationMaleToFemale: Boolean = config.getBoolean("job.is-m-to-f-ratio")

  def getDbName: String = config.getString("db.name")

  def getCollMillionaires: String = config.getString("db.collections.millionaires")
  def getCollPopultaion: String = config.getString("db.collections.popultaion")
  def getCollRatio: String = config.getString("db.collections.ratio")
  def getCollTop: String = config.getString("db.collections.top")

  def getTestDbName: String = config.getString("db.test-db-name")
  def getTestDataPath: String = config.getString("test-data")

  def getDbServer: String = config.getString("db.server")
  def getDbPort: Int = config.getInt("db.port")
}
