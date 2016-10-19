package teleporter.integration.core

import akka.actor.{ActorRef, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.file.FilePublisher
import teleporter.integration.component.jdbc.JdbcPublisher
import teleporter.integration.component.mongo.MongoPublisher
import teleporter.integration.component.{KafkaPublisher, ShadowPublisher}

import scala.collection.concurrent.TrieMap
import scala.reflect.{ClassTag, classTag}

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait TeleporterSource extends SourceMetadata with LazyLogging {
  var sourceApply = TrieMap[String, Class[_]](
    "kafka" → classOf[KafkaPublisher],
    "dataSource" → classOf[JdbcPublisher],
    "mongo" → classOf[MongoPublisher],
    "file" → classOf[FilePublisher]
  )

  def apply[T](id: Long)(implicit center: TeleporterCenter): Source[T, ActorRef] = apply[T](center.context.getContext[SourceContext](id).key)

  def apply[T](key: String)(implicit center: TeleporterCenter): Source[T, ActorRef] = {
    val system = center.system
    val context = center.context.getContext[SourceContext](key)
    implicit val config = context.config
    val sourceProps = Props(sourceApply(lnsCategory), key, center)
    val props = lnsShadow match {
      case true ⇒
        Props(classOf[ShadowPublisher], key, system.actorOf(sourceProps), center)
      case false ⇒ sourceProps
    }
    Source.actorPublisher[T](props).mapMaterializedValue {
      ref ⇒ context.actorRef = ref; ref
    }
  }

  def registerType[T <: ActorPublisher[_] : ClassTag](category: String): Unit = {
    sourceApply += category → classTag[T].runtimeClass
  }
}

class TeleporterSourceImpl extends TeleporterSource

object TeleporterSource {
  def apply(): TeleporterSource = new TeleporterSourceImpl
}