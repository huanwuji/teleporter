package teleporter.task.tests

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import teleporter.integration.core.TeleporterCenter

import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2016/3/2.
 */
class Counter()(implicit val center: TeleporterCenter) extends Actor {

  import context.dispatcher

  val registry = center.metricsRegistry
  context.system.scheduler.schedule(1.seconds, 10.millis, self, 'Add)
  val counterMetrics = registry.counter("counter")

  override def receive: Receive = {
    case 'Add ⇒ counterMetrics.inc()
  }
}

object MetricsTest extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()

  import system.dispatcher

  val center = TeleporterCenter()
  center.openMetrics(5.seconds)
  system.actorOf(Props(classOf[Counter], center))
}
