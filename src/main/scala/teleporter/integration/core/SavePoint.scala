package teleporter.integration.core

import kafka.common.TopicAndPartition
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.fbs.{EventStatus, EventType}
import teleporter.integration.cluster.rpc.{EventBody, TeleporterEvent}
import teleporter.integration.component.Kafka.KafkaLocation
import teleporter.integration.utils.{Jackson, MapBean}

import scala.concurrent.Future

/**
  * author: huanwuji
  * created: 2015/9/4.
  */
object SavePoint {
  def empty[T]: SavePoint[T] = new EmptySavePoint[T]

  def sourceSavePoint()(implicit center: TeleporterCenter) = new SourceSavePointImpl()

  def kafkaSavePoint(consumerConnector: ZkKafkaConsumerConnector)(implicit center: TeleporterCenter) = new KafkaSavePoint(consumerConnector)
}

trait SavePoint[T] {
  def save(key: String, point: T): Future[Boolean]

  def complete(key: String, point: T): Future[Boolean]
}

class EmptySavePoint[T] extends SavePoint[T] {
  override def save(key: String, point: T): Future[Boolean] = Future.successful(true)

  override def complete(key: String, point: T): Future[Boolean] = Future.successful(true)
}

trait RecoverStreamStatus extends Logging {
  def recoverStream(key: String)(implicit center: TeleporterCenter): Option[Future[Boolean]] = {
    import center.system.dispatcher
    val context = center.context.getContext[StreamContext](key)
    val streamMetaBean = context.config.mapTo[StreamMetaBean]
    if (StreamStatus.FAILURE eq streamMetaBean.status) {
      logger.info(s"Will recover $key status to Normal")
      val result = center.eventListener.asyncEvent { seqNr ⇒
        center.brokers ! SendMessage(
          TeleporterEvent.request(seqNr = seqNr, eventType = EventType.AtomicSaveKV,
            body = EventBody.AtomicKV(key = key,
              expect = Jackson.mapper.writeValueAsString(streamMetaBean.toMap),
              update = Jackson.mapper.writeValueAsString(streamMetaBean.status(StreamStatus.NORMAL))))
        )
      }._2.map(_.status == EventStatus.Success)
      Some(result)
    } else None
  }
}

class SourceSavePointImpl()(implicit center: TeleporterCenter)
  extends SavePoint[MapBean] with RecoverStreamStatus {

  import center.system.dispatcher

  override def save(key: String, point: MapBean): Future[Boolean] = {
    val context = center.context.getContext[SourceContext](key)
    val streamSaveResult = recoverStream(context.stream().key)

    def saveSource(): Future[Boolean] = {
      center.eventListener.asyncEvent { seqNr ⇒
        center.brokers ! SendMessage(
          TeleporterEvent.request(seqNr = seqNr, eventType = EventType.AtomicSaveKV,
            body = EventBody.AtomicKV(key = key,
              expect = Jackson.mapper.writeValueAsString(context.config.toMap),
              update = Jackson.mapper.writeValueAsString(point.toMap)))
        )
      }._2.map(_.status == EventStatus.Success)
    }

    streamSaveResult.map {
      fu ⇒
        fu.flatMap {
          streamResult ⇒ if (streamResult) saveSource() else Future.successful(false)
        }
    }.getOrElse(saveSource())
  }

  override def complete(key: String, point: MapBean): Future[Boolean] = {
    logger.info(s"Finish $key commit: $point")
    save(key, point)
  }
}

object KafkaSavePoint {
  def apply(consumerConnector: ZkKafkaConsumerConnector)(implicit center: TeleporterCenter): KafkaSavePoint = new KafkaSavePoint(consumerConnector)
}

class KafkaSavePoint(consumerConnector: ZkKafkaConsumerConnector)(implicit center: TeleporterCenter)
  extends SavePoint[KafkaLocation] with RecoverStreamStatus with Logging {

  import center.system.dispatcher

  override def save(key: String, point: KafkaLocation): Future[Boolean] = {
    val context = center.context.getContext[SourceContext](key)
    val streamSaveResult = recoverStream(context.stream().key)

    def saveSource(): Future[Boolean] = {
      val topicPartition = point.topicPartition
      consumerConnector.commitOffsets(TopicAndPartition(topicPartition.topic, topicPartition.partition), point.offset)
      logger.info(s"$key commit kafka offset: $point")
      Future.successful(true)
    }

    streamSaveResult.map {
      fu ⇒
        fu.flatMap {
          streamResult ⇒ if (streamResult) saveSource() else Future.successful(false)
        }
    }.getOrElse(saveSource())
  }

  override def complete(key: String, point: KafkaLocation): Future[Boolean] = {
    save(key, point)
    logger.info(s"Finish $key commit kafka offset: $point")
    Future.successful(true)
  }
}