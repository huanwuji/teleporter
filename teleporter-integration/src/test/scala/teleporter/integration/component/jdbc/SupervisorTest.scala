package teleporter.integration.component.jdbc

import akka.actor.SupervisorStrategy.Resume
import akka.actor._

import scala.concurrent.duration._

/**
 * author: huanwuji
 * created: 2015/8/12.
 */
object SupervisorTest extends App {
  implicit val system = ActorSystem()

  import system.dispatcher

  class A extends Actor with ActorLogging {
    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 2) {
      case _ => println("An actor has been killed"); Resume
    }
    val b = context.actorOf(Props[B], "b")
    context.system.scheduler.schedule(10 seconds, 1 seconds, b, true)


    def receive = {
      case true ⇒ println("is resume"); throw new IllegalArgumentException()
      case _ =>
    }
  }

  class B extends Actor with ActorLogging {
    def receive = {
      case true => throw new IllegalArgumentException("exception")
    }
  }

  system.actorOf(Props[A])
}
