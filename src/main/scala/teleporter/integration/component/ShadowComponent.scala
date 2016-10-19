package teleporter.integration.component

import akka.actor.ActorRef
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.ShadowPublisher.Register
import teleporter.integration.core.{Component, TId, TeleporterCenter, TeleporterMessage}

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Author: kui.dai
  * Date: 2015/12/21.
  */
object ShadowComponent

case class ShadowRequest(n: Long)

/**
  * It's used for heavily component which will often restart it's stream, make steam bind it's shadow
  *
  * @param key    sourceId
  * @param center center
  */
class ShadowPublisher(override val key: String, subjectActorRef: ActorRef)(implicit val center: TeleporterCenter)
  extends ActorPublisher[TeleporterMessage[Any]] with Component with LazyLogging {

  val buffer = mutable.Queue[TeleporterMessage[Any]]()

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    subjectActorRef ! Register(self)
  }

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    subjectActorRef ! Register(self)
    logger.error(reason.getLocalizedMessage, reason)
  }

  override def receive: Receive = {
    case Request(n) ⇒ delivery(n)
    case tId: TId ⇒ subjectActorRef ! tId
    case message: TeleporterMessage[Any] ⇒
      if (totalDemand == 0) {
        buffer.enqueue(message)
      } else {
        onNext(message)
      }
    case ActorPublisherMessage.Cancel ⇒ context.stop(self)
    case x ⇒ logger.warn(s"$key Can't arrived, $x")
  }

  @tailrec
  final def delivery(n: Long): Unit = {
    if (n > 0)
      if (buffer.nonEmpty) {
        onNext(buffer.dequeue())
        delivery(n - 1)
      } else {
        subjectActorRef ! ShadowRequest(n)
      }
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    logger.info(s"Shadow component $key will stop")
  }
}

object ShadowPublisher {

  case class Register(shadowRef: ActorRef)

}