package teleporter.integration

import javax.sql.DataSource

import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import org.elasticsearch.action.update.UpdateRequest
import org.mongodb.scala.Document
import teleporter.integration.component.jdbc.Action
import teleporter.integration.component.kudu.KuduAction
import teleporter.integration.core.TeleporterMessage

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
package object component {
  type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]
  type KafkaRecord = ProducerRecord[Array[Byte], Array[Byte]]
  type TeleporterKafkaRecord = TeleporterMessage[KafkaRecord]
  type TeleporterKafkaMessage = TeleporterMessage[KafkaMessage]
  type JdbcMessage = Map[String, Any]
  type TeleporterJdbcMessage = TeleporterMessage[JdbcMessage]
  type JdbcRecord = Seq[Action]
  type JdbcFunction = DataSource ⇒ Unit
  type TeleporterJdbcRecord = TeleporterMessage[JdbcRecord]
  type TeleporterJdbcFunction = TeleporterMessage[JdbcFunction]
  type ElasticRecord = UpdateRequest
  type TeleporterElasticRecord = TeleporterMessage[ElasticRecord]
  type MongoMessage = Document
  type TeleporterMongoMessage = TeleporterMessage[MongoMessage]
  type KuduRecord = KuduAction
  type TeleporterKuduRecord = TeleporterMessage[KuduRecord]
}