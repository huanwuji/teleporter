package teleporter.task.tests

import akka.actor._
import akka.pattern._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Author: kui.dai
 * Date: 2015/12/17.
 */
class Publisher extends ActorPublisher[Int] with ActorLogging {
  val child = context.actorOf(Props(new Actor {
    override def receive: Actor.Receive = {
      case "start" ⇒ Thread.sleep(10000000)
    }

    @throws[Exception](classOf[Exception])
    override def postStop(): Unit = {
      super.postStop()
      log.info("child post stop")
    }
  }))
  var count = 1


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    child ! "start"
  }

  override def receive: Receive = {
    case Request(n) ⇒
      while (totalDemand > 0) {
        onNext(count)
        count += 1
      }
    case "stop" ⇒ child ! Kill; onCompleteThenStop()
    case x ⇒ println(s"publisher $x")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    log.error("parent post stop")
  }
}

class Subscriber extends ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  override def receive: Actor.Receive = {
    case OnNext(ele) ⇒ println(ele)
    case x ⇒ println(s"subscriber $x")
  }
}

object StreamStopTests extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()

  import system.dispatcher

  val publisherActorRef = system.actorOf(Props[Publisher])
  Source(ActorPublisher(publisherActorRef)).runWith(Sink(ActorSubscriber(system.actorOf(Props[Subscriber]))))
  gracefulStop(publisherActorRef, 10.seconds, "stop").onComplete {
    case Success(state) ⇒ println(state)
    case Failure(ex) ⇒ ex.printStackTrace()
  }
}