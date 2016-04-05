package teleporter.integration.metrics

import akka.NotUsed
import akka.actor.{Actor, ActorLogging}
import teleporter.integration.SourceControl.Notify
import teleporter.integration.component.{InfluxDto, InfluxdbClient}
import teleporter.integration.core.TeleporterCenter

import scala.concurrent.duration.FiniteDuration

/**
 * Author: kui.dai
 * Date: 2016/3/1.
 */
class InfluxdbReporter(period: FiniteDuration)(implicit val center: TeleporterCenter) extends Actor with ActorLogging {

  import MetricsImplicits._
  import context.dispatcher

  val client = center.addressing[InfluxdbClient]("metrics:influxdb",NotUsed)

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
    client.save(dto)(client, dispatcher)
  }
}
