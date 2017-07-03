package utils

import com.mongodb.casbah.commons.MongoDBObject

/**
  * Created by meamuri on 03.07.17.
  */
object DatabaseUtils {
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
