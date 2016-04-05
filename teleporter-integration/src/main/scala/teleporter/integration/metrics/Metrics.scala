package teleporter.integration.metrics

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging}
import com.codahale.metrics._
import nl.grons.metrics.scala.{HdrMetricBuilder, InstrumentedBuilder}
import teleporter.integration.Scheduled
import teleporter.integration.component.{InfluxDto, InfluxdbClient}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2015/12/10.
 */
class Metrics

trait HdrInstrumentedBuilder extends InstrumentedBuilder {
  override lazy protected val metricBuilder = new HdrMetricBuilder(metricBaseName, metricRegistry, resetAtSnapshot = true)
}

class InfluxdbMetricsReporter(period: FiniteDuration, registry: MetricRegistry)(implicit client: InfluxdbClient) extends Actor with ActorLogging {

  import context.dispatcher

  val clock = Clock.defaultClock
  context.system.scheduler.schedule(period, period, self, Scheduled)

  override def receive: Receive = {
    case Scheduled ⇒ reports(registry)
  }

  def reports(registry: MetricRegistry): Unit = {
    val timestamp: Long = TimeUnit.MILLISECONDS.toSeconds(clock.getTime)
    registry.getGauges.foreach(t2 ⇒ report(t2._1, "gauge", t2._2, timestamp))
    registry.getCounters.foreach(t2 ⇒ report(t2._1, "counter", t2._2, timestamp))
    registry.getHistograms.foreach(t2 ⇒ report(t2._1, "histogram", t2._2, timestamp))
    registry.getMeters.foreach(t2 ⇒ report(t2._1, "meter", t2._2, timestamp))
    registry.getTimers.foreach(t2 ⇒ report(t2._1, "timer", t2._2, timestamp))
  }

  def report(name: String, category: String, data: Any, timestamp: Long): Unit = {
    val dto = InfluxDto(name, Map(category → data), System.currentTimeMillis())
    client.save(dto)
  }
}