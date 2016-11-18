package teleporter.integration.core

import akka.actor.{ActorRef, Props}
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.file.FilePublisher
import teleporter.integration.component.jdbc.JdbcPublisher
import teleporter.integration.component.mongo.MongoPublisher
import teleporter.integration.component.{KafkaPublisher, ShadowPublisher}

import scala.collection.concurrent.TrieMap

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait TeleporterSource extends SourceMetadata with LazyLogging {
  var sourceApplies = TrieMap[String, (String, TeleporterCenter) ⇒ Props](
    "kafka" → { (key, center) ⇒ Props(classOf[KafkaPublisher], key, center) },
    "dataSource" → { (key, center) ⇒ Props(classOf[JdbcPublisher], key, center).withDispatcher("akka.teleporter.blocking-io-dispatcher") },
    "mongo" → { (key, center) ⇒ Props(classOf[MongoPublisher], key, center) },
    "file" → { (key, center) ⇒ Props(classOf[FilePublisher], key, center) }
  )

  def apply[T](id: Long)(implicit center: TeleporterCenter): Source[T, ActorRef] = apply[T](center.context.getContext[SourceContext](id).key)

  def apply[T](key: String)(implicit center: TeleporterCenter): Source[T, ActorRef] = {
    val system = center.system
    val context = center.context.getContext[SourceContext](key)
    implicit val config = context.config
    val sourceProps = sourceApplies(lnsCategory)(key, center)
    val props = lnsShadow match {
      case true ⇒
        Props(classOf[ShadowPublisher], key, system.actorOf(sourceProps), center)
      case false ⇒ sourceProps
    }
    Source.actorPublisher[T](props).mapMaterializedValue {
      ref ⇒
        center.context.indexes.modifyByKey2(key, _.asInstanceOf[SourceContext].copy(actorRef = ref)); ref
    }
  }

  def registerType(category: String, apply: (String, TeleporterCenter) ⇒ Props): Unit = {
    sourceApplies += category → apply
  }
}

class TeleporterSourceImpl extends TeleporterSource

object TeleporterSource {
  def apply(): TeleporterSource = new TeleporterSourceImpl
}