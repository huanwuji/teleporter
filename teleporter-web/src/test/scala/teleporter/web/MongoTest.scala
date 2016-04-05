package teleporter.web

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.scalatest.FunSuite

/**
 * Author: kui.dai
 * Date: 2015/11/11.
 */
class MongoTest extends FunSuite {

  case class SourceConf(
                         id: Option[Long] = None,
                         taskId: Option[Long] = None,
                         addressId: Option[Long] = None,
                         category: String,
                         name: String,
                         props: Map[String, Any])

  test("mongo") {
    import scala.concurrent.ExecutionContext.Implicits.global
    val client: MongoClient = MongoClient("mongodb://172.18.2.156:27017")
    val database: MongoDatabase = client.getDatabase("teleporter_web")
    database.listCollectionNames().toFuture().foreach(println)
    val collection = database.getCollection("source")
    val source = collection.find(equal("id", 222)).toFuture()
    source.foreach {
      case Nil ⇒ println(true)
      case _ ⇒ println(false)
    }
    Thread.sleep(5000)
  }
}