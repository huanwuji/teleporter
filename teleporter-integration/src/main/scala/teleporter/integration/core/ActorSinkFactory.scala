package teleporter.integration.core

import akka.actor.{ActorSystem, Props}
import akka.stream.actor.ActorSubscriber
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.Subscriber
import teleporter.integration.component.{ElasticSubscriber, KafkaSubscriber}
import teleporter.integration.component.jdbc.JdbcSubscriber
import teleporter.integration.conf.Conf
import teleporter.integration.core.conf.TeleporterConfigFactory

/**
 * date 2015/8/3.
 * @author daikui
 */
trait ActorSinkFactory extends LazyLogging {
  implicit val teleporterCenter: TeleporterCenter
  implicit val system: ActorSystem
  var types = Map[String, Class[_]](
    "kafka" → classOf[KafkaSubscriber],
    "dataSource" → classOf[JdbcSubscriber],
    "elasticsearch" -> classOf[ElasticSubscriber]
  )

  def loadConf(id: Int): Conf.Sink

  def build[T](id: Int): Subscriber[T] = {
    build[T](id, loadConf(id))
  }

  def build[T](id: Int, conf: Conf.Sink): Subscriber[T] = {
    logger.info(s"Init sink, id:$id, conf:$conf")
    val actorRef = system.actorOf(Props(types(conf.category), id, teleporterCenter), id.toString)
    teleporterCenter.actorAddresses.register(id, actorRef)
    ActorSubscriber[T](actorRef)
  }

  def registerType[T, A <: Subscriber[T]](sinkType: String, clazz: Class[A]): Unit = {
    types = types + (sinkType → clazz)
  }
}

object ActorSinkFactory {
  def apply(teleporterConfigFactory: TeleporterConfigFactory)
           (implicit _teleporterCenter: TeleporterCenter, _system: ActorSystem) = {
    new ActorSinkFactory {
      override implicit val teleporterCenter: TeleporterCenter = _teleporterCenter
      override implicit val system: ActorSystem = _system

      override def loadConf(id: Int): Conf.Sink = teleporterConfigFactory.sink(id)
    }
  }
}