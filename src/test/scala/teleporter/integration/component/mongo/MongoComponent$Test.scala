package teleporter.integration.component.mongo

import java.util.Date

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.bson.json.{JsonMode, JsonWriterSettings}
import org.mongodb.scala.bson._
import org.scalatest.FunSuite

/**
 * Author: kui.dai
 * Date: 2016/2/18.
 */
//case class Person(id: Long, name: String, birth: Date)

class MongoComponent$Test extends FunSuite {
  test("bson to json") {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.registerModule(new Bson2JsonModule)
    mapper.registerModule(DefaultScalaModule)
    val bsonDoc = BsonDocument(
      "id" → BsonObjectId(),
      "string" → BsonString("aa"),
      "int" → BsonInt32(3434),
      "long" → BsonInt64(43343L),
      "double" → BsonDouble(3434.33f),
      "date" → BsonDateTime(new Date())
    )
    println(bsonDoc.toJson(new JsonWriterSettings(JsonMode.SHELL)))
    //    println(bsonDoc.toJson(new JsonWriterSettings()))
    val str = mapper.writeValueAsString(bsonDoc)
    println(str)
  }
  test("json to bson") {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.registerModule(new Json2BsonModule)
    mapper.registerModule(DefaultScalaModule)
    val map = Map("int" → 3434, "long" → 3433333333333333343L, "float" → 434.33f, "double" → 343.4343d, "date" → new Date())
    println(mapper.writeValueAsString(map))
  }
  test("str to bson") {
    val bson = BsonDocument(
      """
        |{id:3434343434333}
      """.stripMargin)
    println(bson)
  }
}