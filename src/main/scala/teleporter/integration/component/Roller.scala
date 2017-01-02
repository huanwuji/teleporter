package teleporter.integration.component

import java.time.{LocalDateTime, Duration ⇒ JDuration}

import scala.collection.Iterator._
import scala.collection.{AbstractIterator, GenTraversableOnce, Iterator}
import scala.concurrent.duration._

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait Roller[A] extends Iterator[A]

case class RollPage(currPage: Int, pageSize: Int)

case class PageRoller(var page: Int, pageSize: Int, maxPage: Int = Int.MaxValue) extends Roller[RollPage] {
  private val initPage = page
  val self: PageRoller = this
  private var totalCount = 0

  override def hasNext: Boolean = page <= maxPage && ((page - initPage) * pageSize == totalCount)

  override def next(): RollPage = {
    val currPage = page
    page += 1
    RollPage(currPage, pageSize)
  }

  override def flatMap[B](f: (RollPage) ⇒ GenTraversableOnce[B]): scala.Iterator[B] = new AbstractIterator[B] {
    private var cur: Iterator[B] = empty

    def hasNext: Boolean = cur.hasNext || self.hasNext && {
      cur = f(self.next()).toIterator
      cur.hasNext
    }

    def next(): B = {
      totalCount += 1
      (if (hasNext) cur else empty).next()
    }
  }
}

case class RollTime(start: LocalDateTime, end: LocalDateTime)

case class TimeRoller(var start: LocalDateTime, deadline: () ⇒ LocalDateTime, period: Duration, maxPeriod: Duration, isContinuous: Boolean) extends Roller[RollTime] {
  override def hasNext: Boolean = JDuration.between(start, deadline()).toNanos >= period.toNanos

  override def next(): RollTime = {
    val deadlineTime = deadline()
    val distance = JDuration.between(start, deadlineTime).toNanos
    val nanos = distance min maxPeriod.toNanos min period.toNanos
    val end = start.plusNanos(nanos)
    val rollTime = RollTime(start = start, end = end)
    start = end
    rollTime
  }
}