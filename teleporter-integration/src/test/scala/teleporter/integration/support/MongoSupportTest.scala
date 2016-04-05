package teleporter.integration.support

import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.FunSuite

/**
 * Author: kui.dai
 * Date: 2016/2/22.
 */
class MongoSupportTest extends FunSuite {
  test("map to bson") {
    println(Document("{\"status\":\"COMPLETE\"}"))
  }
}
