package teleporter.integration.core

import java.time.{Duration ⇒ JDuration, LocalDateTime}

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import teleporter.integration._
import teleporter.integration.conf.Conf.Props
import teleporter.integration.conf.{Conf, PageRollerProps, TimePollerProps, TimeRollerProps}
import teleporter.integration.transaction.BatchCommitTransaction

import scala.annotation.tailrec
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

  import TimePollerProps._

  def apply[T](props: Props, handler: Props ⇒ Iterator[T]) = {
    new Rollers[T](
      props = props,
      handler = handler,
      isContinuous = TimeRoller.isContinuous(props),
      isTimerRoller = TimeRoller.isTimerRoller(props),
      isPageRoller = PageRoller.isPageRoller(props)
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

  def isPageRoller(props: PageRollerProps) = props.pageSize.isDefined

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

  def isContinuous(props: TimeRollerProps): Boolean = props.deadline.exists(_.contains("fromNow"))

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
        val dateTime = LocalDateTime.parse(dateTimeStr, DEFAULT_DATE_FORMATTER)
        () ⇒ dateTime
      case _ ⇒ throw new IllegalArgumentException(s"deadline is required, $props")
    }
    val start = LocalDateTime.parse(props.start.get, DEFAULT_DATE_FORMATTER)
    val period = Duration(props.period.get)
    val maxPeriod = props.maxPeriod.map(Duration(_)).getOrElse(period)
    TimeRoller(
      start = start,
      deadline = deadline,
      period = period,
      maxPeriod = maxPeriod,
      isContinuous = isContinuous(props)
    )
  }

  def isTimerRoller(props: Props) = props.period.isDefined

  def updateProps(props: Props, rollTime: RollTime): Props = props.start(rollTime.start).end(rollTime.end)

  def timeRollerSchedule(props: Props)(implicit system: ActorSystem, ec: ExecutionContext, actorRef: ActorRef): Cancellable = {
    val periodOpt = props.period
    periodOpt match {
      case Some(period) ⇒
        val duration = Duration(period).asInstanceOf[FiniteDuration]
        system.scheduler.schedule(duration, duration, actorRef, Scheduled)
      case _ ⇒ throw new IllegalArgumentException(s"Cat set period param $props")
    }
  }
}

trait RollerPublisher[T] extends ActorPublisher[TeleporterMessage[T]] with BatchCommitTransaction[T, Conf.Source] with Component with ActorLogging {

  import context.{dispatcher, system}

  var conf: Conf.Source
  private val rollers: Rollers[T] = Rollers(conf.props, _handler)
  private val iterator: Iterator[T] = rollers.iterator
  private var cancellable: Cancellable = null

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    if (rollers.isTimerRoller && rollers.isContinuous) {
      cancellable = TimeRoller.timeRollerSchedule(conf.props)
    }
    super.preStart()
  }

  override def receive: Receive = {
    case Request(n) ⇒ deliver()
    case tId: TId ⇒ end(tId)
    case Scheduled ⇒ deliver()
    case Cancel ⇒ onErrorThenStop(new RuntimeException("sink is closed"))
    case x ⇒ log.warning("can't arrived, {}", x)
  }

  @tailrec
  final def deliver(): Unit = {
    if (totalDemand > 0) {
      val recoveryIt = recovery()
      if (recoveryIt.hasNext) {
        onNext(recoveryIt.next())
        deliver()
      } else {
        if (iterator.hasNext) {
          begin(conf, iterator.next()) {
            msg ⇒ onNext(msg)
          }
          deliver()
        } else {
          if (!rollers.isContinuous) {
            doComplete()
          }
        }
      }
    }
  }

  private def doComplete(): Unit = {
    if (isComplete) {
      onCompleteThenStop()
      complete()
    } else {
      context.system.scheduler.scheduleOnce(5.seconds, self, Scheduled)
    }
  }

  private def _handler(props: Props): Iterator[T] = {
    conf = conf.copy(props = props)
    handler(props)
  }

  def handler(props: Props): Iterator[T]

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    if (cancellable != null) cancellable.cancel()
  }
}