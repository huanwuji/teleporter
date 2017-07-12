package teleporter.integration.core

import kafka.common.TopicAndPartition
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.rpc.EventBody
import teleporter.integration.cluster.rpc.fbs.{MessageStatus, MessageType}
import teleporter.integration.component.Kafka.KafkaLocation
import teleporter.integration.core.TeleporterContext.Update
import teleporter.integration.utils.{Jackson, MapBean, MapMetaBean}

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
  def recoverStream(key: String)(implicit center: TeleporterCenter): Future[Option[Boolean]] = {
    import center.system.dispatcher
    val context = center.context.getContext[StreamContext](key)
    val streamMetaBean = context.config
    if (StreamStatus.NORMAL ne streamMetaBean.status) {
      logger.info(s"Will recover $key status to Normal")
      val targetStreamConfig = streamMetaBean.status(StreamStatus.NORMAL)
      center.brokerConnection.future.flatMap(_.handler.request(MessageType.AtomicSaveKV,
        EventBody.AtomicKV(key = key,
          expect = Jackson.mapper.writeValueAsString(streamMetaBean.toMap),
          update = Jackson.mapper.writeValueAsString(targetStreamConfig)).toArray)(center.eventListener)
      ).map { m ⇒
        if (m.status == MessageStatus.Success) {
          center.context.ref ! Update(context.copy(config = MapMetaBean[StreamMetaBean](targetStreamConfig)))
        }
        Some(m.status == MessageStatus.Success)
      }
    } else Future.successful(None)
  }
}

class SourceSavePointImpl()(implicit center: TeleporterCenter)
  extends SavePoint[MapBean] with RecoverStreamStatus {

  import center.system.dispatcher

  override def save(key: String, point: MapBean): Future[Boolean] = {
    val context = center.context.getContext[SourceContext](key)
    recoverStream(context.stream().key).flatMap { r ⇒
      if (r.exists(!_)) {
        Future.successful(false)
      } else {
        center.brokerConnection.future.flatMap(_.handler.request(MessageType.AtomicSaveKV,
          EventBody.AtomicKV(key = key,
            expect = Jackson.mapper.writeValueAsString(context.config.toMap),
            update = Jackson.mapper.writeValueAsString(point.toMap)).toArray)(center.eventListener)
        ).map(_.status == MessageStatus.Success)
      }
    }
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
    recoverStream(context.stream().key).flatMap { r ⇒
      if (r.exists(!_)) {
        Future.successful(false)
      } else {
        val topicPartition = point.topicPartition
        consumerConnector.commitOffsets(TopicAndPartition(topicPartition.topic, topicPartition.partition), point.offset)
        logger.info(s"$key commit kafka offset: $point")
        Future.successful(true)
      }
    }
  }

  override def complete(key: String, point: KafkaLocation): Future[Boolean] = {
    save(key, point)
    logger.info(s"Finish $key commit kafka offset: $point")
    Future.successful(true)
  }
}