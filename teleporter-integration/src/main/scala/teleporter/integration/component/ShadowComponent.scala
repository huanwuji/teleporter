package teleporter.integration.component

import akka.actor.ActorLogging
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import teleporter.integration.SourceControl.{CompleteThenStop, ErrorThenStop, Register}
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
 * @param id sourceId
 * @param center center
 */
class ShadowPublisher(override val id: Int)(implicit center: TeleporterCenter)
  extends ActorPublisher[TeleporterMessage[Any]] with Component with ActorLogging {

  val buffer = mutable.Queue[TeleporterMessage[Any]]()
  val sourceActorRef = center.actor(id)


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    sourceActorRef ! Register(self)
  }

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    sourceActorRef ! Register(self)
    log.error(reason, s"${reason.getMessage}: ${message.toString}")
  }

  override def receive: Receive = {
    case Request(n) ⇒ delivery(n)
    case tId: TId ⇒ sourceActorRef ! tId
    case message: TeleporterMessage[Any] ⇒
      if (totalDemand == 0) {
        buffer.enqueue(message)
      } else {
        onNext(message)
      }
    case ErrorThenStop(reason: Throwable) ⇒ onErrorThenStop(reason)
    case CompleteThenStop ⇒ onCompleteThenStop()
    case Cancel ⇒ context.stop(self)
    case x ⇒ log.warning(s"$id Can't arrived, $x")
  }

  @tailrec
  final def delivery(n: Long): Unit = {
    if (n > 0)
      if (buffer.nonEmpty) {
        onNext(buffer.dequeue())
        delivery(n - 1)
      } else {
        sourceActorRef ! ShadowRequest(n)
      }
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    log.info(s"Shadow component $id will stop")
  }
}