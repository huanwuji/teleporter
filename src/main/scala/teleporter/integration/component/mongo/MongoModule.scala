package teleporter.integration.component.mongo

import java.time.format.DateTimeFormatter
import java.util.Date

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.{JsonGenerator, Version}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.module.SimpleSerializers
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.mongodb.scala.bson._

/**
  * Author: kui.dai
  * Date: 2016/2/19.
  */
trait Bson2Json {

  class StringSerializer extends StdScalarSerializer[BsonString](classOf[BsonString], false) {
    override def serialize(t: BsonString, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeString(t.getValue)
  }

  class BooleanSerializer extends StdScalarSerializer[BsonBoolean](classOf[BsonBoolean], false) {
    override def serialize(t: BsonBoolean, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeBoolean(t.getValue)
  }

  class BsonDateTimeSerializer extends StdScalarSerializer[BsonDateTime](classOf[BsonDateTime], false) {
    override def serialize(t: BsonDateTime, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeNumber(t.getValue)
  }

  class BsonObjectIdSerializer extends StdScalarSerializer[BsonObjectId](classOf[BsonObjectId], false) {
    override def serialize(t: BsonObjectId, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeString(t.getValue.toString)
  }

  class BsonInt32Serializer extends StdScalarSerializer[BsonInt32](classOf[BsonInt32], false) {
    override def serialize(t: BsonInt32, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeNumber(t.getValue)
  }

  class BsonInt64Serializer extends StdScalarSerializer[BsonInt64](classOf[BsonInt64], false) {
    override def serialize(t: BsonInt64, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeNumber(t.getValue)
  }

  class BsonDoubleSerializer extends StdScalarSerializer[BsonDouble](classOf[BsonDouble], false) {
    override def serialize(t: BsonDouble, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeNumber(t.getValue)
  }

  class NullSerializer extends StdScalarSerializer[BsonNull](classOf[BsonNull], false) {
    override def serialize(t: BsonNull, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeNull()
  }

}

class Bson2JsonModule extends Module with Bson2Json {
  def getModuleName: String = "Bson2JsonModule"

  def version: Version = new Version(2, 6, 3, "", "com.teleporter", "bson2json")

  def setupModule(context: Module.SetupContext) {
    val bson2JsonSerializers = new SimpleSerializers
    bson2JsonSerializers.addSerializer(classOf[BsonString], new StringSerializer)
    bson2JsonSerializers.addSerializer(classOf[BsonDateTime], new BsonDateTimeSerializer)
    bson2JsonSerializers.addSerializer(classOf[BsonObjectId], new BsonObjectIdSerializer)
    bson2JsonSerializers.addSerializer(classOf[BsonInt32], new BsonInt32Serializer)
    bson2JsonSerializers.addSerializer(classOf[BsonInt64], new BsonInt64Serializer)
    bson2JsonSerializers.addSerializer(classOf[BsonDouble], new BsonDoubleSerializer)
    bson2JsonSerializers.addSerializer(classOf[BsonBoolean], new BooleanSerializer)
    bson2JsonSerializers.addSerializer(classOf[BsonNull], new NullSerializer)
    context.addSerializers(bson2JsonSerializers)
  }
}

object Bson2Json {
  def apply(): ObjectMapper with ScalaObjectMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
      .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .registerModule(new Bson2JsonModule)
      .registerModule(DefaultScalaModule)
    mapper
  }
}

trait Json2Bson {

  class DateSerializer extends StdScalarSerializer[Date](classOf[Date], false) {
    override def serialize(t: Date, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
      val dateStr = DateTimeFormatter.ISO_INSTANT.format(t.toInstant)
      jsonGenerator.writeRawValue( s"""ISODate("$dateStr")""")
    }
  }

  class LongSerializer extends StdScalarSerializer[java.lang.Long](classOf[java.lang.Long], false) {
    override def serialize(t: java.lang.Long, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeRawValue(s" NumberLong($t)")
  }

  class BsonDoubleSerializer extends StdScalarSerializer[BsonDouble](classOf[BsonDouble], false) {
    override def serialize(t: BsonDouble, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = jsonGenerator.writeNumber(t.getValue)
  }

}

class Json2BsonModule extends Module with Json2Bson {
  def getModuleName: String = "Json2BsonModule"

  def version: Version = new Version(2, 6, 3, "", "com.teleporter", "json2Bson")

  def setupModule(context: Module.SetupContext) {
    val json2BsonSerializers = new SimpleSerializers
    json2BsonSerializers.addSerializer(classOf[java.lang.Long], new LongSerializer)
    json2BsonSerializers.addSerializer(classOf[Date], new DateSerializer)
    context.addSerializers(json2BsonSerializers)
  }
}

object Json2Bson {
  def apply(): ObjectMapper with ScalaObjectMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
      .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .registerModule(new Json2BsonModule)
      .registerModule(DefaultScalaModule)
    mapper
  }
}

class MongoModule