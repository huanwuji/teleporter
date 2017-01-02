package teleporter.integration.metrics

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.{InfluxDto, InfluxdbClient}
import teleporter.integration.core.TeleporterCenter
import teleporter.integration.metrics.InfluxdbReporter.Notify
import teleporter.integration.metrics.Metrics.Measurement

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

  val client: InfluxdbClient = center.components.address[InfluxdbClient](key)

  context.system.scheduler.schedule(period, period, self, Notify)

  override def receive: Receive = {
    case Notify ⇒ reports(center.metricsRegistry)
  }

  def reports(registry: MetricRegistry): Unit = {
    val timestamp: Long = System.currentTimeMillis()
    registry.metrics.foreach {
      case (measurement, metrics) ⇒
        metrics match {
          case counter: MetricsCounter ⇒ report(measurement, counter.dump(), timestamp)
          case timer: MetricsTimer ⇒ report(measurement, timer.dump(), timestamp)
        }
    }
  }

  def report(measurement: Measurement, data: Map[String, Any], timestamp: Long): Unit = {
    val dto = InfluxDto(measurement, data, System.currentTimeMillis())
    client.save(dto).onComplete {
      case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
      case _ ⇒
    }
  }
}
