package initial

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject

/**
  * инициализация noSQL базы данных MongoDB
  */
object MongoInit {

  private val mongoClient =  MongoClient("casbah_text")

  def getMongoClient: MongoClient = { mongoClient }

  def testDatabase(): Unit = {
    val obj = MongoDBObject("foo" -> "bar",
      "x" -> "y",
      "pie" -> 3.14
    )
    val builder = MongoDBObject.newBuilder
    builder += "foo" -> "bar"
    builder += "x" -> "y"
    builder += ("pie" -> 3.14)
    builder += ("spam" -> "eggs", "mmm" -> "bacon")

    val newObj = builder.result
    println(newObj)
  }
}
