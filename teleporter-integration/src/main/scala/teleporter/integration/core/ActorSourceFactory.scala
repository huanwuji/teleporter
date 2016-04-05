package teleporter.integration.core

import akka.actor.{ActorSystem, Props}
import akka.stream.actor.ActorPublisher
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.{Publisher, Subscriber}
import teleporter.integration.component.jdbc.JdbcPublisher
import teleporter.integration.component.{ForwardProducer, KafkaPublisher}
import teleporter.integration.conf.Conf
import teleporter.integration.core.conf.TeleporterConfigFactory

/**
 * date 2015/8/3.
 * @author daikui
 */
trait ActorSourceFactory extends LazyLogging {
  implicit val teleporterCenter: TeleporterCenter
  implicit val system: ActorSystem
  var types = Map[String, Class[_]](
    "kafka" → classOf[KafkaPublisher],
    "forward" → classOf[ForwardProducer],
    "dataSource" → classOf[JdbcPublisher]
  )

  def loadConf(id: Int): Conf.Source

  def build[T](id: Int): Publisher[T] = {
    build[T](id, loadConf(id))
  }

  def build[T](id: Int, conf: Conf.Source): Publisher[T] = {
    logger.info(s"Init source, id:$id, conf:$conf")
    val actorRef = system.actorOf(Props(types(conf.category), id, teleporterCenter), id.toString)
    teleporterCenter.actorAddresses.register(id, actorRef)
    ActorPublisher[T](actorRef)
  }

  def registerType[T, A <: Subscriber[T]](sourceName: String, clazz: Class[A]): Unit = {
    types = types + (sourceName → clazz)
  }
}

object ActorSourceFactory {
  def apply(teleporterConfigFactory: TeleporterConfigFactory)
           (implicit _teleporterCenter: TeleporterCenter, _system: ActorSystem): ActorSourceFactory = {
    new ActorSourceFactory {
      override implicit val teleporterCenter: TeleporterCenter = _teleporterCenter

      override implicit val system: ActorSystem = _system

      override def loadConf(id: Int): Conf.Source = teleporterConfigFactory.source(id)
    }
  }
}