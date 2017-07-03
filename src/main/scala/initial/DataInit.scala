package initial

import java.io.File

import com.github.tototoshi.csv.CSVReader
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject

/**
  *
  */
object DataInit {
  def initCsv(): Unit = {
    val reader = CSVReader.open(new File("data/unsd-citypopulation-year-both.csv"))
    val info = reader.allWithHeaders()

    println(info.length)
    println(info.take(5))

    reader.close()
  }

  def initDataBase(): Unit = {
    val mongoClient =  MongoClient("casbah_text")
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
