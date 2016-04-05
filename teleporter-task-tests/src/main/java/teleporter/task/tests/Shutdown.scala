package teleporter.task.tests

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2016/1/13.
 */
object Completed

class ShutdownPublisher extends Actor with ActorPublisher[Int] {
  var count = 1

  override def receive: Receive = {
    case Request(n) ⇒ while (totalDemand > 0) {
      onNext(count)
      Thread.sleep(10000)
      count += 1
    }
    case Completed ⇒ onCompleteThenStop()
    case x ⇒ println(x)
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    println("Is stop")
  }
}

object Shutdown extends App with LazyLogging {
  implicit val system = ActorSystem()
  val decider: Supervision.Decider = {
    case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.stop
  }
  implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
  val actorRef = system.actorOf(Props[ShutdownPublisher])
  Source(1 to 100).map(x ⇒ {
    Thread.sleep(5000); x
  }).completionTimeout(2.seconds).runForeach(println)
  //  try {
  //    Source.fromPublisher(ActorPublisher[Int](actorRef))
  //      .idleTimeout(2.seconds)
  //      .completionTimeout(2.seconds)
  //      .map {
  //        x ⇒
  //          println(s"map: $x")
  //          //          Thread.sleep(5000)
  //          x
  //      }
  //      .runForeach(println)
  //  } catch {
  //    case e: Exception ⇒ e.printStackTrace()
  //  }

  //  Source.fromPublisher(ActorPublisher[Int](system.actorOf(Props[Publisher])))
  //    .map {
  //      x ⇒
  //        println(s"map: $x")
  //        Thread.sleep(5000)
  //        x
  //    }
  //    .runForeach(println)
  //  Source(1 to 10).map {
  //    x ⇒
  //      println(s"map: $x")
  //      Thread.sleep(1000)
  //      x
  //  }
  //    .runForeach(println)
  //  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
  //    override def run(): Unit =
  //      try {
  //        println(222222)
  //        println(Await.result(system.terminate(), 1.minutes))
  //        Thread.sleep(5000)
  //        println(333333)
  //        //        system.terminate().onSuccess {
  //        //          case v ⇒ println(v)
  //        //        }
  //      } catch {
  //        case e: Exception ⇒ e.printStackTrace()
  //      }
  //  }))
}
