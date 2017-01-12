package teleporter.integration.core

import kafka.common.TopicAndPartition
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.broker.PersistentProtocol.AtomicKeyValue
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.TeleporterEvent
import teleporter.integration.cluster.rpc.fbs.generate.{EventType, Role}
import teleporter.integration.component.Kafka.KafkaLocation
import teleporter.integration.utils.{Jackson, MapBean}

/**
  * author: huanwuji
  * created: 2015/9/4.
  */
object CheckPoint {
  def empty[T]: CheckPoint[T] = new EmptyCheckPoint[T]

  def sourceCheckPoint()(implicit center: TeleporterCenter) = new SourceCheckPointImpl()

  def kafkaCheckPoint(consumerConnector: ZkKafkaConsumerConnector)(implicit center: TeleporterCenter) = new KafkaCheckPoint(consumerConnector)
}

trait CheckPoint[T] {
  def save(key: String, point: T): Unit

  def complete(key: String): Unit
}

class EmptyCheckPoint[T] extends CheckPoint[T] {
  override def save(key: String, point: T): Unit = {}

  override def complete(key: String): Unit = {}
}

class SourceCheckPointImpl()(implicit center: TeleporterCenter) extends CheckPoint[MapBean] {
  override def save(key: String, point: MapBean): Unit = {
    val sourceContext = center.context.getContext[SourceContext](key)
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.AtomicSaveKV, role = Role.CLIENT,
          body = AtomicKeyValue(key = key,
            expect = Jackson.mapper.writeValueAsString(sourceContext.config.toMap),
            update = Jackson.mapper.writeValueAsString(point.toMap)))
      )
    }
  }

  override def complete(key: String): Unit = {
    val streamConfig = center.context.getContext[StreamContext](key).config
    val targetStreamConfig = streamConfig.status(StreamStatus.COMPLETE)
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.AtomicSaveKV, role = Role.CLIENT,
          body = AtomicKeyValue(key = key,
            expect = Jackson.mapper.writeValueAsString(streamConfig.toMap),
            update = Jackson.mapper.writeValueAsString(targetStreamConfig)))
      )
    }
  }
}

object KafkaCheckPoint {
  def apply(consumerConnector: ZkKafkaConsumerConnector): KafkaCheckPoint = new KafkaCheckPoint(consumerConnector)
}

class KafkaCheckPoint(consumerConnector: ZkKafkaConsumerConnector) extends CheckPoint[KafkaLocation] with Logging {
  override def save(key: String, point: KafkaLocation): Unit = {
    val topicPartition = point.topicPartition
    consumerConnector.commitOffsets(TopicAndPartition(topicPartition.topic, topicPartition.partition), point.offset)
    logger.info(s"$key commit kafka offset: $point")
  }

  override def complete(key: String): Unit = {}
}