package teleporter.integration.metrics

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.{InfluxDto, InfluxdbClient}
import teleporter.integration.core.TeleporterCenter
import teleporter.integration.metrics.InfluxdbReporter.Notify

import scala.concurrent.duration.FiniteDuration
import scala.util.Failure

/**
  * Author: kui.dai
  * Date: 2016/3/1.
  */
object InfluxdbReporter {

  case object Notify

}

class InfluxdbReporter(key: String, period: FiniteDuration)(implicit val center: TeleporterCenter) extends Actor with LazyLogging {

  import MetricsImplicits._
  import center.materializer
  import context.dispatcher

  val client = center.components.address[InfluxdbClient](key)

  context.system.scheduler.schedule(period, period, self, Notify)

  override def receive: Receive = {
    case Notify ⇒ reports(center.metricsRegistry)
  }

  def reports(registry: MetricRegistry): Unit = {
    val timestamp: Long = System.currentTimeMillis()
    registry.metrics.foreach {
      case t2@(name, metrics) ⇒
        metrics match {
          case counter: MetricsCounter ⇒ report(name, counter.dump(), timestamp)
          case timer: MetricsTimer ⇒ report(name, timer.dump(), timestamp)
        }
    }
  }

  def report(name: String, data: Map[String, Any], timestamp: Long): Unit = {
    val dto = InfluxDto(name, data, System.currentTimeMillis())
    client.save(dto).onComplete {
      case Failure(e) ⇒ logger.error(e.getLocalizedMessage)
      case _ ⇒
    }
  }
}
