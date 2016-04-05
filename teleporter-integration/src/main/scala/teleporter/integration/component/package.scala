package teleporter.integration

import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import teleporter.integration.component.jdbc.Action
import teleporter.integration.core.TeleporterMessage
import teleporter.integration.proto.FileBuf.FileProto

/**
 * date 2015/8/3.
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
  type TeleporterJdbcRecord = TeleporterMessage[JdbcRecord]
  type ElasticRecord = ElasticComponent.ElasticRecord
  type TeleporterElasticRecord = TeleporterMessage[ElasticRecord]
  type FileRecord = FileProto
  type TeleporterFileRecord = TeleporterMessage[FileRecord]

  object Control {

    object CompleteThenStop

    case class ErrorThenStop(reason: Throwable)

    object StopWhenHaveData

  }

}