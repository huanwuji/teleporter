package teleporter.integration.core

import akka.actor.Props
import akka.stream.actor.ActorPublisher
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.Publisher
import teleporter.integration.CmpType
import teleporter.integration.component.file.FilePublisher
import teleporter.integration.component.jdbc.JdbcPublisher
import teleporter.integration.component.mongo.MongoPublisher
import teleporter.integration.component.{KafkaPublisher, ShadowPublisher}
import teleporter.integration.conf.Conf
import teleporter.integration.conf.Conf.Source
import teleporter.integration.core.conf.TeleporterConfigFactory

/**
 * date 2015/8/3.
 *
 * @author daikui
 */
trait ActorSourceFactory extends LazyLogging {
  var types = Map[String, Class[_]](
    "kafka" → classOf[KafkaPublisher],
    "dataSource" → classOf[JdbcPublisher],
    "mongo" → classOf[MongoPublisher],
    "file"→ classOf[FilePublisher]
  )

  def loadConf(id: Int): Conf.Source

  def build[T](id: Int)(implicit center: TeleporterCenter): Publisher[T] = {
    build[T](id, loadConf(id))
  }

  def build[T](id: Int, conf: Conf.Source)(implicit center: TeleporterCenter): Publisher[T] = {
    logger.info(s"Init source, id:$id, conf:$conf")
    val system = center.system
    conf.category.split(":") match {
      case Array("shadow", cat) ⇒
        if (!center.actorAddresses.exists(id, CmpType.Source)) {
          val actorRef = system.actorOf(Props(types(cat), id, center), id.toString)
          center.actorAddresses.register(id, actorRef, CmpType.Source)
        }
        val shadowRef = system.actorOf(Props(classOf[ShadowPublisher], id, center), s"$id-shadow")
        center.actorAddresses.register(id, shadowRef, CmpType.Shadow)
        ActorPublisher[T](shadowRef)
      case _ ⇒
        val actorRef = system.actorOf(Props(types(conf.category), id, center), id.toString)
        center.actorAddresses.register(id, actorRef, CmpType.Source)
        ActorPublisher[T](actorRef)
    }
  }

  def registerType[A <: ActorPublisher[_]](category: String)(implicit manifest: Manifest[A]): Unit = {
    types = types + (category → manifest.runtimeClass)
  }
}

class ActorSourceFactoryImpl(configFactory: TeleporterConfigFactory) extends ActorSourceFactory {
  override def loadConf(id: Int): Source = configFactory.source(id)
}

object ActorSourceFactory {
  def apply(configFactory: TeleporterConfigFactory) = new ActorSourceFactoryImpl(configFactory)
}