package teleporter.integration.transaction

import com.typesafe.scalalogging.LazyLogging
import kafka.common.TopicAndPartition
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.mongodb.scala.MongoDatabase
import teleporter.integration.component.KafkaComponent.KafkaLocation
import teleporter.integration.conf.Conf
import teleporter.integration.conf.Conf.{Source, StreamStatus}

import scala.concurrent.ExecutionContext

/**
 * author: huanwuji
 * created: 2015/9/4.
 */
trait RecoveryPoint[T] {
  def save(persistenceId: Int, batchInfo: BatchInfo[T]): Unit

  def complete(persistenceId: Int): Unit
}

object RecoveryPoint {
  val empty = new EmptyRecoveryPoint
}

class EmptyRecoveryPoint extends RecoveryPoint[Conf.Source] {
  override def save(persistenceId: Int, batchInfo: BatchInfo[Conf.Source]): Unit = {}

  override def complete(persistenceId: Int): Unit = {}
}

class KafkaRecoveryPoint(consumerConnector: ZkKafkaConsumerConnector) extends RecoveryPoint[KafkaLocation] with LazyLogging {
  override def save(persistenceId: Int, point: BatchInfo[KafkaLocation]): Unit = {
    for (batchCoordinate ← point.batchCoordinates) {
      val topicPartition = batchCoordinate.topicPartition
      consumerConnector.commitOffsets(TopicAndPartition(topicPartition.topic, topicPartition.partition), batchCoordinate.offset - 1)
      logger.info(s"$persistenceId commit kafka offset: $batchCoordinate")
    }
  }

  override def complete(persistenceId: Int): Unit = {}
}

object KafkaRecoveryPoint {
  def apply(consumerConnector: ZkKafkaConsumerConnector): KafkaRecoveryPoint = new KafkaRecoveryPoint(consumerConnector)
}

class MongoStoreRecoveryPoint()(implicit db: MongoDatabase, ec: ExecutionContext) extends RecoveryPoint[Conf.Source] with LazyLogging {
  override def save(persistenceId: Int, point: BatchInfo[Source]): Unit =
    for (source ← point.batchCoordinates) {
      Conf.Source.save(source)
      logger.info(s"commit id:$persistenceId, $source")
    }

  override def complete(persistenceId: Int): Unit = Conf.Source.modify(persistenceId, Map("status" → StreamStatus.COMPLETE))
}