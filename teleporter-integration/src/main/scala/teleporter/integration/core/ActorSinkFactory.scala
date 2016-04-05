package teleporter.integration.core

import akka.actor.Props
import akka.stream.actor.ActorSubscriber
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.Subscriber
import teleporter.integration.CmpType
import teleporter.integration.component.jdbc.JdbcSubscriber
import teleporter.integration.component.{ElasticSubscriber, KafkaSubscriber}
import teleporter.integration.conf.Conf.Sink
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core.conf.TeleporterConfigFactory

/**
 * date 2015/8/3.
 * @author daikui
 */
trait ActorSinkFactory extends PropsSupport with LazyLogging {
  var types = Map[String, Class[_]](
    "kafka" → classOf[KafkaSubscriber],
    "dataSource" → classOf[JdbcSubscriber],
    "elasticsearch" -> classOf[ElasticSubscriber]
  )

  def loadConf(id: Int): Conf.Sink

  def build[T](id: Int)(implicit center: TeleporterCenter): Subscriber[T] = {
    build[T](id, loadConf(id))
  }

  def build[T](id: Int, conf: Conf.Sink)(implicit center: TeleporterCenter): Subscriber[T] = {
    logger.info(s"Init sink, id:$id, conf:$conf")
    val isPrototype = getBooleanOrElse(conf.props, "prototype", default = false)
    val actorRef = if (isPrototype) {
      center.system.actorOf(Props(types(conf.category), id, center))
    } else {
      val _actorRef = center.system.actorOf(Props(types(conf.category), id, center), id.toString)
      center.actorAddresses.register(id, _actorRef, CmpType.Sink)
      _actorRef
    }
    ActorSubscriber[T](actorRef)
  }

  def registerType[T, A <: Subscriber[T]](category: String, clazz: Class[A]): Unit = {
    types = types + (category → clazz)
  }
}

class ActorSinkFactoryImpl(configFactory: TeleporterConfigFactory) extends ActorSinkFactory {
  override def loadConf(id: Int): Sink = configFactory.sink(id)
}

object ActorSinkFactory {
  def apply(configFactory: TeleporterConfigFactory) = new ActorSinkFactoryImpl(configFactory)
}