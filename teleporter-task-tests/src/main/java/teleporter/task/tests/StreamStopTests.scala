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
class StreamPublisher extends ActorPublisher[Int] with ActorLogging {
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
    context.stop(self)
    //    child ! "start"
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

class StreamSubscriber extends ActorSubscriber {
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

  val publisherActorRef = system.actorOf(Props[StreamPublisher])
  val publisherActorRef1 = system.actorOf(Props[StreamPublisher])

  val aa = Source.actorPublisher[Int](Props[StreamPublisher]).runWith(Sink.fromSubscriber(ActorSubscriber(system.actorOf(Props[StreamSubscriber]))))
  aa
  Sink.queue()
  Thread.sleep(10000)
  println("--------------")
  gracefulStop(publisherActorRef, 20.seconds, "stop").onComplete {
    case Success(state) ⇒ println(state)
    case Failure(ex) ⇒ ex.printStackTrace()
  }
}