package teleporter.integration

import javax.sql.DataSource

import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import org.elasticsearch.action.update.UpdateRequest
import org.mongodb.scala.Document
import teleporter.integration.component.jdbc.Action
import teleporter.integration.component.kudu.KuduAction

/**
  * Created by huanwuji 
  * date 2017/1/12.
  */
package object component {
  type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]
  type KafkaRecord = ProducerRecord[Array[Byte], Array[Byte]]
  type JdbcMessage = Map[String, Any]
  type JdbcRecord = Seq[Action]
  type JdbcFunction = DataSource ⇒ Unit
  type ElasticRecord = UpdateRequest
  type MongoMessage = Document
  type KuduRecord = KuduAction
}
