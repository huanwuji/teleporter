package teleporter.integration.script

import akka.util.ByteString
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, SerializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import teleporter.integration.component._


/**
 * Created by joker  on 15/12/9
 */
trait MsgParser[T] {

  def parser(t: T):Any

}


case class MessageTmp(headers: Map[String, Any], payload: Map[String, Any])


object KafkaMsgParser extends MsgParser[KafkaMessage]() {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
  mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  override def parser(msg: KafkaMessage): MessageTmp = {
    val message: String = ByteString(msg.toString).utf8String
    val headers = mapper.readValue[Map[String,Any]](message).get("headers").get.toString
    val headerMap = mapper.readValue[Map[String,Any]](headers)
    val payload = mapper.readValue[Map[String,Any]](message).get("payload").get.toString
    val msgMap = mapper.readValue[Map[String,Any]](payload)
    new MessageTmp(headerMap, msgMap)
  }
}


object MsgParser {

  implicit def parse(msg: KafkaMessage): MessageTmp = KafkaMsgParser.parser(msg)

}


