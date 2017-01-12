package teleporter.integration.metrics

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.google.common.base.Stopwatch
import teleporter.integration.metrics.Metrics.{Count, Measurement, Time}

import scala.collection.concurrent.TrieMap

/**
  * Author: kui.dai
  * Date: 2015/12/10.
  */
trait Metrics[T] {
  def dump(): T
}

object Metrics {

  case class Count(count: Long, start: Long, end: Long)

  case class Time(count: Long, totalSpend: Long, min: Long, max: Long)

  case class Tag(key: String, value: String)

  object Tags {
    val success = Tag("status", "SUCCESS")
    val error = Tag("status", "ERROR")
  }

  case class Measurement(name: String, tags: Seq[Tag] = Seq.empty) {
    val str = s"$name${if (tags.isEmpty) "" else "," + tags.map(tag ⇒ s"${tag.key}=${tag.value}").mkString(",")}"
  }

  def count[T](name: String)(implicit registry: MetricRegistry): Flow[T, T, NotUsed] = {
    count[T](Measurement(name))
  }

  def count[T](measurement: Measurement)(implicit registry: MetricRegistry): Flow[T, T, NotUsed] = {
    val metrics = registry.counter(measurement)
    Flow[T].map[T] { t ⇒ metrics.inc(); t }
  }

  def time[T](name: String, f: ⇒ T)(implicit registry: MetricRegistry): Flow[T, T, NotUsed] = {
    time[T](Measurement(name), f)
  }

  def time[T](measurement: Measurement, f: ⇒ T)(implicit registry: MetricRegistry): Flow[T, T, NotUsed] = {
    val metrics = registry.timer(measurement)
    Flow[T].map[T] { t ⇒ metrics.time(f); t }
  }
}

object MetricsImplicits {
  implicit def count2Map(count: Count): Map[String, Any] = Map(
    "count" → count.count,
    "start" → count.start,
    "end" → count.end
  )

  implicit def time2Map(time: Time): Map[String, Any] = Map(
    "count" → time.count,
    "totalSpend" → time.totalSpend,
    "min" → time.min,
    "max" → time.max
  )
}

class MetricsCounter extends Metrics[Count] {
  val count = new AtomicLong()
  var current: Long = System.currentTimeMillis()

  def inc(inc: Long = 1L): Unit = {
    count.addAndGet(inc)
  }

  def dec(dec: Long = -1L): Unit = {
    count.addAndGet(dec)
  }

  def dump(): Count = {
    val prev = current
    current = System.currentTimeMillis()
    Count(count.getAndSet(0), prev, current)
  }
}

class MetricsTimer extends Metrics[Time] {
  val count = new AtomicLong()
  var min = 0L
  var max = 0L
  var totalSpend = 0L

  def time[A](f: ⇒ A): A = {
    count.incrementAndGet()
    val stopwatch = Stopwatch.createStarted()
    try {
      f
    } finally {
      stopwatch.stop
      val spend = stopwatch.elapsed(TimeUnit.NANOSECONDS)
      if (min > spend || min == 0) min = spend
      if (max < spend) max = spend
      totalSpend += spend
    }
  }

  def dump(): Time = {
    val (_min, _max, _totalSpend) = (min, max, totalSpend)
    min = 0L
    max = 0L
    totalSpend = 0L
    Time(count.getAndSet(0L), _totalSpend, _min, _max)
  }
}

class MetricRegistry {
  val metrics: TrieMap[Measurement, Any] = TrieMap[Measurement, Any]()

  def counter(measurement: Measurement): MetricsCounter = metrics.getOrElseUpdate(measurement, new MetricsCounter).asInstanceOf[MetricsCounter]

  def timer(measurement: Measurement): MetricsTimer = metrics.getOrElseUpdate(measurement, new MetricsTimer).asInstanceOf[MetricsTimer]
}

object MetricRegistry {
  def apply(): MetricRegistry = new MetricRegistry()
}