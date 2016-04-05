package teleporter.task.tests

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorPublisher, ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import akka.stream.scaladsl.{Sink, Source}

/**
 * Author: kui.dai
 * Date: 2016/2/25.
 */
class Publisher extends ActorPublisher[Long] {
  var seqNr = 0

  override def receive: Receive = {
    case Request(n) ⇒
      for (i ← 1L to n) {
        seqNr += 1
        onNext(seqNr)
      }
      if (seqNr == 5) {
        onCompleteThenStop()
      }
    case "Shutdown" ⇒ onCompleteThenStop()
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("publisher stop")
  }
}

class Subscriber extends ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  override def receive: Actor.Receive = {
    case OnNext(ele) ⇒ println(ele)
    case OnComplete ⇒ println("onComplete"); context.stop(self)
    case OnError(e) ⇒ println(e)
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("subscriber stop")
  }
}

object OnCompleteTest extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  //  Source.actorPublisher[Long](Props[Publisher]).runWith(Sink.actorSubscriber(Props[Subscriber]))
  Source(1L to 10L).runWith(Sink.actorSubscriber(Props[Subscriber]))
}
