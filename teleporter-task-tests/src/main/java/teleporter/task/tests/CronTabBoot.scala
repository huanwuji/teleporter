package teleporter.task.tests

import akka.actor._
import com.markatta.akron.{CronExpression, CronTab}

/**
 * Author: kui.dai
 * Date: 2015/10/26.
 */
object CronTabBoot extends App {
  val system = ActorSystem("system")

  val crontab = system.actorOf(CronTab.props, "crontab")

  val someOtherActor = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case "woo" ⇒ println("this code is executed")
    }
  }), "test")

  // send woo to someOtherActor once every minute
  crontab ! CronTab.Schedule(someOtherActor, "woo", CronExpression("* 8 * * *"))

  class DeadLetterListener extends Actor {
    override def receive: Actor.Receive = {
      case x ⇒ println(x)
    }
  }

  val actorRef = system.actorOf(Props[DeadLetterListener])
  //  system.eventStream.subscribe(actorRef, classOf[SuppressedDeadLetter])
  system.eventStream.subscribe(actorRef, classOf[AllDeadLetters])
}