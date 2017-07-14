package unit

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCollection}
import factories.Resources
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Тесты работы с базой MongoDB
  */
class MongoSuite extends FunSuite with BeforeAndAfter{
  private val mongoClient = MongoClient(Resources.getDbServer, Resources.getDbPort)
  private val db = mongoClient(Resources.getTestDbName)

  val const_collection: MongoCollection = db("const")
  var some_collection: MongoCollection = _


  before {
    some_collection = db("coll")
  }

  after {
    val cursor = some_collection.find()
    while (cursor.hasNext){
      some_collection.remove(cursor.next())
    }
  }

  test("collection should be empty!") {
    assert(some_collection.count() === 0)
  }

  test("insert into collections should increment count to 1 element") {
    val builder = MongoDBObject.newBuilder
    builder += "Poet" -> "L Cohen"
    builder += "Poem" -> "Dance me to the end of Love"
    val res = builder.result

    some_collection.insert(res)
    assert(some_collection.count() === 1)
  }
}
