package teleporter.integration.metrics

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.google.common.base.Stopwatch

import scala.collection.concurrent.TrieMap

/**
  * Author: kui.dai
  * Date: 2015/12/10.
  */
case class Count(count: Long, start: Long, end: Long)

case class Time(count: Long, totalSpend: Long, min: Long, max: Long)

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

trait Metrics[T] {
  def dump(): T
}

class MetricsCounter extends Metrics[Count] {
  val count = new AtomicLong()
  var current = System.currentTimeMillis()

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
  val metrics = TrieMap[String, Any]()

  def counter(name: String): MetricsCounter = metrics.getOrElseUpdate(name, new MetricsCounter).asInstanceOf[MetricsCounter]

  def timer(name: String): MetricsTimer = metrics.getOrElseUpdate(name, new MetricsTimer).asInstanceOf[MetricsTimer]
}

object MetricRegistry {
  def apply(): MetricRegistry = new MetricRegistry()
}