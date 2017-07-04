package utils

import com.mongodb.casbah.commons.MongoDBObject

/**
  * Некоторый функционал по работе с MongoDB
  */
object MongoUtils {
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
