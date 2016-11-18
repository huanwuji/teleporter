package teleporter.integration.core

import akka.actor.{ActorRef, Props}
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.Subscriber
import teleporter.integration.component.jdbc.JdbcSubscriber
import teleporter.integration.component.{ElasticSubscriber, KafkaSubscriber}

import scala.reflect.{ClassTag, classTag}

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait TeleporterSink extends SinkMetadata with LazyLogging {
  var sinkApply = Map[String, Class[_]](
    "kafka" → classOf[KafkaSubscriber],
    "dataSource" → classOf[JdbcSubscriber],
    "elasticsearch" → classOf[ElasticSubscriber]
  )

  def apply[T](id: Long)(implicit center: TeleporterCenter): Sink[T, ActorRef] = apply[T](center.context.getContext[SinkContext](id).key)

  def apply[T](key: String)(implicit center: TeleporterCenter): Sink[T, ActorRef] = {
    logger.info(s"Init sink, $key")
    val context = center.context.getContext[SinkContext](key)
    implicit val config = context.config
    Sink.actorSubscriber[T](Props(sinkApply(lnsCategory), key, center))
      .mapMaterializedValue {
        ref ⇒
          center.context.indexes.modifyByKey2(key, _.asInstanceOf[SinkContext].copy(actorRef = ref)); ref
      }
  }

  def registerType[T <: Subscriber[_] : ClassTag](category: String): Unit = {
    sinkApply += category → classTag[T].runtimeClass
  }
}

class TeleporterSinkImpl extends TeleporterSink

object TeleporterSink {
  def apply(): TeleporterSink = new TeleporterSinkImpl
}