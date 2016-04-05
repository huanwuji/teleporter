package teleporter.integration.component

import java.time.{Duration ⇒ JDuration, LocalDateTime}

import akka.actor._
import teleporter.integration._
import teleporter.integration.conf.Conf.Props
import teleporter.integration.conf._
import teleporter.integration.utils.Dates

import scala.collection.Iterator._
import scala.collection.{AbstractIterator, GenTraversableOnce, Iterator}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Roller[A] extends Iterator[A]

class Rollers[T](props: Props, handler: Props ⇒ Iterator[T], val isContinuous: Boolean,
                 val isTimerRoller: Boolean, val isPageRoller: Boolean) extends Iterable[T] {

  override def iterator: scala.Iterator[T] = {
    import TimePollerProps._
    if (isTimerRoller && isPageRoller) {
      val timeRoller = TimeRoller(props)
      timeRoller.flatMap {
        rollTime ⇒ pageRoller(TimeRoller.updateProps(this.props, rollTime), handler)
      }
    } else if (isTimerRoller) {
      val timeRoller = TimeRoller(props)
      timeRoller.flatMap {
        rollTime ⇒
          val props = TimeRoller.updateProps(this.props, rollTime)
          handler(props)
      }
    } else if (isPageRoller) {
      pageRoller(props, handler)
    } else {
      handler(props)
    }
  }

  private def pageRoller(_props: Props, handler: Props ⇒ Iterator[T]): Iterator[T] = {
    import PageRollerProps._
    PageRoller(_props).flatMap {
      rollPage ⇒ handler(PageRoller.updateProps(_props, rollPage))
    }
  }
}

object Rollers {

  import RollerProps._

  def apply[T](props: Props, handler: Props ⇒ Iterator[T]) = {
    val rollerProps: RollerProps = props
    new Rollers[T](
      props = props,
      handler = handler,
      isContinuous = rollerProps.isContinuous,
      isTimerRoller = rollerProps.isTimerRoller,
      isPageRoller = rollerProps.isPageRoller
    )
  }
}

case class RollPage(currPage: Int, pageSize: Int)

case class PageRoller(var page: Int, pageSize: Int, maxPage: Int = Int.MaxValue) extends Roller[RollPage] {
  private val initPage = page
  val self = this
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

object PageRoller {

  import PageRollerProps._

  def apply(props: PageRollerProps): Iterator[RollPage] = {
    PageRoller(
      page = props.page,
      pageSize = props.pageSize.get,
      maxPage = props.maxPage
    )
  }

  def updateProps(props: Props, rollPage: RollPage): Props = props.page(rollPage.currPage)
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

object TimeRoller {

  import TimePollerProps._

  def apply(props: TimeRollerProps): TimeRoller = {
    val deadline: () ⇒ LocalDateTime = props.deadline match {
      case Some("now") ⇒
        val now = LocalDateTime.now(); () ⇒ now
      case Some("fromNow") ⇒
        () ⇒ LocalDateTime.now()
      case Some(offsetDeadline) if offsetDeadline.endsWith(".fromNow") ⇒ //1.minutes.fromNow
        val duration = Duration(offsetDeadline.substring(0, offsetDeadline.lastIndexOf(".")))
        () ⇒ LocalDateTime.now().minusSeconds(duration.toSeconds)
      case Some(dateTimeStr) ⇒
        val dateTime = LocalDateTime.parse(dateTimeStr, Dates.DEFAULT_DATE_FORMATTER)
        () ⇒ dateTime
      case _ ⇒ throw new IllegalArgumentException(s"deadline is required, $props")
    }
    val start = LocalDateTime.parse(props.start.get, Dates.DEFAULT_DATE_FORMATTER)
    val period = Duration(props.period.get)
    val maxPeriod = props.maxPeriod.map(Duration(_)).getOrElse(period)
    TimeRoller(
      start = start,
      deadline = deadline,
      period = period,
      maxPeriod = maxPeriod,
      isContinuous = props.isContinuous
    )
  }

  def updateProps(props: Props, rollTime: RollTime): Props = props.start(rollTime.start).end(rollTime.end)

  def timeRollerSchedule(props: Props)(implicit system: ActorSystem, ec: ExecutionContext, actorRef: ActorRef): Cancellable = {
    val periodOpt = props.period
    periodOpt match {
      case Some(period) ⇒
        val duration = Duration(period).asInstanceOf[FiniteDuration]
        system.scheduler.schedule(duration, duration, actorRef, SourceControl.Notify)
      case _ ⇒ throw new IllegalArgumentException(s"Cat set period param $props")
    }
  }
}