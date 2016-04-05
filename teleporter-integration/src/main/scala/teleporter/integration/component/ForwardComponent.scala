package teleporter.integration.component

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import teleporter.integration.core.{Component, TeleporterCenter}

import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
class ForwardProducer(override val id: Int)(implicit teleporterCenter: TeleporterCenter) extends ActorPublisher[Any] with Component {
  val queue = mutable.Queue[Any]()

  override def receive: Receive = {
    case Request(n) ⇒ deliver()
    case x ⇒ if (totalDemand == 0) queue.enqueue(x) else onNext(x)
  }

  def deliver(): Unit = {
    while (totalDemand > 0 && queue.nonEmpty) {
      onNext(queue.dequeue())
    }
  }
}

object ForwardComponent