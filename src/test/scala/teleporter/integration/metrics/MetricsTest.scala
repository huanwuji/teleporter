package teleporter.integration.metrics

import java.util.concurrent.locks.LockSupport

import akka.actor.{Actor, ActorSystem, Props}
import org.scalatest.FunSuite

import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2015/12/8.
 */
class Counter(val registry: MetricRegistry) extends Actor {

  import context.dispatcher

  context.system.scheduler.schedule(1.seconds, 10.millis, self, 'Add)
  val counterMetrics = registry.counter("counter")

  override def receive: Receive = {
    case 'Add ⇒ counterMetrics.inc()
  }
}

class MetricsTest extends FunSuite {
  val metricRegistry = new MetricRegistry()
  implicit val system = ActorSystem()

  test("init") {
    val counter = system.actorOf(Props(classOf[Counter], metricRegistry))
    system.actorOf(Props(classOf[InfluxdbReporter], 1.seconds, metricRegistry))
    LockSupport.park()
  }
}