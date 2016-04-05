package teleporter.task.tests

import akka.actor._
import com.markatta.akron.CronTab
import teleporter.integration.core.TId

/**
 * Author: kui.dai
 * Date: 2015/10/26.
 */
object TidTest extends App {
  val system = ActorSystem("system")

  val crontab = system.actorOf(CronTab.props, "crontab")

  val someOtherActor = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case "woo" ⇒ println("this code is executed")
      case tid: TId ⇒ println(tid)
    }
  }), "test")
  someOtherActor ! TId(1, 1, 1)
}