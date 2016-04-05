package teleporter.integration.transaction

import com.typesafe.scalalogging.LazyLogging
import kafka.common.TopicAndPartition
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.mongodb.scala.MongoDatabase
import teleporter.integration.component.KafkaComponent.KafkaLocation
import teleporter.integration.conf.Conf
import teleporter.integration.conf.Conf.{Source, SourceStatus}
import teleporter.integration.core.TeleporterCenter
import teleporter.integration.core.conf.LocalTeleporterConfigStore

import scala.concurrent.ExecutionContext

/**
 * author: huanwuji
 * created: 2015/9/4.
 */
trait RecoveryPoint[T] {
  def save(persistenceId: Int, batchInfo: BatchInfo[T]): Unit

  def complete(persistenceId: Int): Unit

  def recovery(persistenceId: Int): Option[T]
}

object RecoveryPoint {
  val empty = new EmptyRecoveryPoint
}

class EmptyRecoveryPoint extends RecoveryPoint[Any] {
  override def save(persistenceId: Int, batchInfo: BatchInfo[Any]): Unit = {}

  override def complete(persistenceId: Int): Unit = {}

  override def recovery(persistenceId: Int): Option[Any] = None
}

class KafkaRecoveryPoint(consumerConnector: ZkKafkaConsumerConnector) extends RecoveryPoint[KafkaLocation] with LazyLogging {
  override def save(persistenceId: Int, point: BatchInfo[KafkaLocation]): Unit = {
    for (batchCoordinate ← point.batchCoordinates) {
      consumerConnector.commitOffsets(TopicAndPartition(batchCoordinate.topic, batchCoordinate.partition), batchCoordinate.offset)
      logger.info(s"$persistenceId commit kafka offset: $batchCoordinate")
    }
  }

  override def recovery(persistenceId: Int): Option[KafkaLocation] = Some(new KafkaLocation("empty", 0, 0))

  override def complete(persistenceId: Int): Unit = {}
}

object KafkaRecoveryPoint {
  def apply(consumerConnector: ZkKafkaConsumerConnector): KafkaRecoveryPoint = new KafkaRecoveryPoint(consumerConnector)
}

class MongoStoreRecoveryPoint()(implicit teleporterCenter: TeleporterCenter, db: MongoDatabase, ec: ExecutionContext) extends RecoveryPoint[Conf.Source] with LazyLogging {
  override def save(persistenceId: Int, point: BatchInfo[Source]): Unit =
    for (source ← point.batchCoordinates) {
      Conf.Source.save(source)
      logger.info(s"commit id:$persistenceId, $source")
    }

  override def complete(persistenceId: Int): Unit = Conf.Source.modify(persistenceId, Map("status" → SourceStatus.COMPLETE))

  override def recovery(persistenceId: Int): Option[Conf.Source] = Conf.Source.getOption(persistenceId)
}

class LocalStoreRecoveryPoint(localStore: LocalTeleporterConfigStore) extends RecoveryPoint[Conf.Source] with LazyLogging {
  override def save(persistenceId: Int, batchInfo: BatchInfo[Source]): Unit =
    for (source ← batchInfo.batchCoordinates) {
      localStore.sources += (persistenceId → source.copy(status = SourceStatus.COMPLETE))
      logger.info(s"commit id:$persistenceId, $source")
    }

  override def complete(persistenceId: Int): Unit = logger.info(s"persistenceId: $persistenceId is completed")

  override def recovery(persistenceId: Int): Option[Source] = None
}