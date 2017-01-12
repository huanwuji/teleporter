package teleporter.integration.cluster.broker.mongo

import org.mongodb.scala.MongoClient
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by huanwuji on 2016/12/1.
  */
class MongoDBServiceTest extends FunSuite {
  val mongoClient = MongoClient("mongodb://172.18.21.213:27017")
  val database = mongoClient.getDatabase("test")
  test("mongo atomic save") {
    val mongoDBService = MongoDBService(database.getCollection("test"))
    println(mongoDBService.atomicPut("a", "e", "d"))
  }
}
