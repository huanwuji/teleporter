package teleporter.integration.component.jdbc

import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import akka.pattern.BackoffSupervisor
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.google.common.util.concurrent.Uninterruptibles
import org.scalatest.FunSuite

import scala.collection.immutable.Queue
import scala.concurrent.duration._

/**
 * date 2015/8/3.
 * @author daikui
 */
class PublisherActor(start: Int) extends ActorPublisher[Int] {
  var iterator = start to 20 toIterator
  var flag = true
  //  override val supervisorStrategy =
  //    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minutes) {
  //      case _: RuntimeException => println("is capture"); Resume
  //    }

  override def receive: Receive = {
    case Request(n) ⇒
      //      while (totalDemand > 0 && iterator.hasNext) {
      //        onNext(iterator.next())
      //      }
      for (i ← 1L to n) {
        if (iterator.hasNext) {
          val curr = iterator.next()
          if (curr == 5 && flag) {
            //            flag = false
            println(s"flag: $flag")
            //                        throw new RuntimeException("Can you process it")
          }
          onNext(curr)
        }
      }
    case "test" ⇒ onErrorThenStop(new RuntimeException())
    case x ⇒ println(s"sss $x")
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("preStart")
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    println(s"postRestart, $reason")
    //    flag = false
    //    iterator = 15 to 20 toIterator
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("postStop")
  }
}

class SubscriberActor extends ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = ZeroRequestStrategy

  import context.dispatcher

  context.system.scheduler.schedule(10.seconds, 10.seconds, self, "request")

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minutes) {
      case _ ⇒ println("subscriber is capture"); Resume
    }


  override def receive: Actor.Receive = {
    case OnNext(i) ⇒
      println(i)
    //      request(1)
    case OnError(cause: Throwable) ⇒ println(s"onError")
    case "request" ⇒ request(1)
    case OnComplete ⇒ println("onComplete")
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("subscriber preStart")
    request(2)
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    println(s"subscriber postRestart, $reason")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("subscriber postStop")
  }
}

class DataSourcePublisherTest extends FunSuite {
  test("error process") {
    val decider: Supervision.Decider = {
      case _ => println("stream resume"); Supervision.Restart
    }
    implicit val system = ActorSystem()

    implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
    //    val pubProps = Props(classOf[PublisherActor], 3)
    //    val publisherProps = BackoffSupervisor.props(
    //      pubProps,
    //      "myEcho",
    //      Duration.create(30, TimeUnit.SECONDS),
    //      Duration.create(30, TimeUnit.SECONDS),
    //      0.2)
    //        implicit val mat = ActorMaterializer()
    val publisherRef = system.actorOf(Props(classOf[PublisherActor], 3))
    //        Source(ActorPublisher(system.actorOf(publisherProps))).to(Sink(ActorSubscriber(subscriberRef))).run()
    //    Source(ActorPublisher(system.actorOf(pubProps))).runForeach(println)
    //        Source(ActorPublisher[Int](publisherRef)).to(Sink(ActorSubscriber[Int](subscriberRef)).withAttributes(ActorAttributes.supervisionStrategy(decider))).run()
    val future = Source.fromPublisher(ActorPublisher(publisherRef)).to(Sink.foreach(println)).run()
    publisherRef ! "test"
    //    Source(1 to 20).to(Sink(ActorSubscriber[Int](subscriberRef))).run()
    //        Await.result(future, 1.minutes)
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES)
  }
  test("actor error") {
    implicit val system = ActorSystem()
    val actorRef = system.actorOf(Props(new Actor {

      import akka.actor.OneForOneStrategy
      import akka.actor.SupervisorStrategy._

      import scala.concurrent.duration._

      override val supervisorStrategy =
        OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minutes) {
          case _: ArithmeticException => Resume
          case _: NullPointerException => Restart
          case _: IllegalArgumentException => println("stop"); Stop
          case _: Exception => Escalate
        }

      override def receive: Actor.Receive = {
        case 1 ⇒ println(1); throw new IllegalArgumentException("trigger error")
      }
    }))
    actorRef ! 1
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES)
  }
  test("Reactive BackoffSupervisor") {
    val decider: Supervision.Decider = {
      case _ => println("stream restart"); Supervision.Resume
    }
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val publishActor = system.actorOf(BackoffSupervisor.props(
      Props(new Publisher),
      "myEcho",
      Duration.create(5, TimeUnit.SECONDS),
      Duration.create(10, TimeUnit.SECONDS),
      0.2))
    val subscribeActor = system.actorOf(BackoffSupervisor.props(
      Props[Subscriber],
      "myEcho",
      Duration.create(5, TimeUnit.SECONDS),
      Duration.create(10, TimeUnit.SECONDS),
      0.2))
    Source.fromPublisher(ActorPublisher[Long](publishActor))
      .to(Sink.fromSubscriber(ActorSubscriber(subscribeActor))).run()
    publishActor ! Cancel
    Thread.sleep(60 * 1000)
  }
  test("BackoffSupervisor") {
    val system = ActorSystem()
    val publishActor = system.actorOf(BackoffSupervisor.propsWithSupervisorStrategy(
      Props[BackOffTestActor],
      "myEcho",
      5.seconds,
      10.seconds,
      0.2, OneForOneStrategy() {
        case e: Exception ⇒ e.printStackTrace(); SupervisorStrategy.Restart
      }))
    //    val publishActor = system.actorOf(Props[BackOffTestActor])
    publishActor ! "stop"
    Thread.sleep(60000)
  }
}

class Publisher extends Actor with ActorPublisher[Long] {
  var count = 0

  override def receive: Actor.Receive = {
    case Request(n: Long) ⇒
      while (totalDemand > 0) {
        count += 1
        println(s"publish $count")
        if (count % 5 == 0) {
          throw new RuntimeException("exception")
        }
        Thread.sleep(1000)
        onNext(count)
      }
    case SubscriptionTimeoutExceeded ⇒ print("timeout")
    case Cancel ⇒ println("Cancel")
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("publisher preStart")
  }


  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    println(s"publisher preRestart,$totalDemand")
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    while (totalDemand > 0) {
      count += 1
      onNext(count)
    }
    println(s"publisher postRestart,$totalDemand, $reason")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("publisher postStop")
  }

}

class Subscriber extends Actor with ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(10, 5)

  override def receive: Actor.Receive = {
    case OnNext(count: Long) ⇒
      if (count % 10 == 0) {
        throw new RuntimeException("subscribe error")
      }
      println(s"subscriber $count")
    case OnError(e) ⇒ e.printStackTrace(); cancel()
    case OnComplete ⇒ println("onComplete")
    case x ⇒ println(s"subscriber msg $x")
  }

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    println(s"subscribe preRestart: $message")
    reason.printStackTrace()
  }
}

class BackOffTestActor extends Actor {
  context.actorOf(Props(new Actor {

    import context.dispatcher

    val aa = this.context.system.scheduler.schedule(5.seconds, 5.seconds, self, "heartbeat")

    override def receive: Actor.Receive = {
      case "heartbeat" ⇒ println("heartbeat")
      case x ⇒ println(x)
    }

    @throws[Exception](classOf[Exception])
    override def postStop(): Unit = {
      println("Child was stop")
      aa.cancel()
    }
  }))

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    reason.printStackTrace()
  }

  override def receive: Actor.Receive = {
    case "error" ⇒ throw new RuntimeException("error")
    case "stop" ⇒ context.stop(self)
    case _ ⇒ println("message")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("Parent was stop")
  }
}