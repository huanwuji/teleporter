package teleporter.integration.utils

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Author: kui.dai
 * Date: 2016/2/23.
 */
object Jackson {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .registerModule(DefaultScalaModule)
}
