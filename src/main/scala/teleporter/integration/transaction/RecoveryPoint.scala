package teleporter.integration.transaction

import com.typesafe.scalalogging.LazyLogging
import kafka.common.TopicAndPartition
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.proto.Rpc.{AtomicKV, EventType, TeleporterEvent}
import teleporter.integration.component.KafkaComponent.KafkaLocation
import teleporter.integration.core.TeleporterConfig.SourceConfig
import teleporter.integration.core.{SourceContext, TeleporterCenter}
import teleporter.integration.utils.Jackson

/**
  * author: huanwuji
  * created: 2015/9/4.
  */
object RecoveryPoint {
  def empty[T]: RecoveryPoint[T] = new EmptyRecoveryPoint[T]

  def apply()(implicit center: TeleporterCenter) = new DefaultRecoveryPoint()
}

trait RecoveryPoint[T] {
  def save(key: String, point: T): Unit

  def complete(key: String): Unit
}

class DefaultRecoveryPoint()(implicit center: TeleporterCenter) extends RecoveryPoint[SourceConfig] {
  override def save(key: String, point: SourceConfig): Unit = {
    val sourceContext = center.context.getContext[SourceContext](key)
    val kv = AtomicKV.newBuilder()
      .setKey(key)
      .setExpect(Jackson.mapper.writeValueAsString(sourceContext.config.toMap))
      .setUpdate(Jackson.mapper.writeValueAsString(point.toMap))
      .build()
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(TeleporterEvent.newBuilder()
        .setRole(TeleporterEvent.Role.CLIENT)
        .setSeqNr(seqNr)
        .setType(EventType.AtomicSaveKV)
        .setBody(kv.toByteString).build())
    }
  }

  override def complete(key: String): Unit = {}
}

class EmptyRecoveryPoint[T] extends RecoveryPoint[T] {
  override def save(key: String, point: T): Unit = {}

  override def complete(key: String): Unit = {}
}

object KafkaRecoveryPoint {
  def apply(consumerConnector: ZkKafkaConsumerConnector): KafkaRecoveryPoint = new KafkaRecoveryPoint(consumerConnector)
}

class KafkaRecoveryPoint(consumerConnector: ZkKafkaConsumerConnector) extends RecoveryPoint[Seq[KafkaLocation]] with LazyLogging {
  override def save(key: String, point: Seq[KafkaLocation]): Unit = {
    for (batchCoordinate ← point) {
      val topicPartition = batchCoordinate.topicPartition
      consumerConnector.commitOffsets(TopicAndPartition(topicPartition.topic, topicPartition.partition), batchCoordinate.offset - 1)
      logger.info(s"$key commit kafka offset: $batchCoordinate")
    }
  }

  override def complete(key: String): Unit = {}
}