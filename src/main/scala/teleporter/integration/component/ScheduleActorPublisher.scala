package teleporter.integration.component

import java.time.{LocalDateTime, Duration ⇒ JDuration}

import akka.actor.Cancellable
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.ScheduleActorPublisherMessage.{PageAttrs, TimeAttrs}
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics.{Measurement, Tags}
import teleporter.integration.metrics.MetricsCounter
import teleporter.integration.transaction.Transaction
import teleporter.integration.utils.{Dates, MapBean, MapMetaBean}

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by kui.dai on 2016/7/11.
  */
object ScheduleMetaBean {
  val FSchedule = "schedule"
  val FPage = "page"
  val FPageSize = "pageSize"
  val FMaxPage = "maxPage"
  val FOffset = "offset"
  val FDeadline = "deadline"
  val FStart = "start"
  val FEnd = "end"
  val FPeriod = "period"
  val FMaxPeriod = "maxPeriod"
}

class ScheduleMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {

  import ScheduleMetaBean._

  def update(pageAttrs: PageAttrs): ScheduleMetaBean = {
    MapMetaBean[ScheduleMetaBean](this ++ (FSchedule, FPage → asString(pageAttrs.page), FOffset → asString(pageAttrs.page * pageAttrs.pageSize)))
  }

  def update(timeAttrs: TimeAttrs): ScheduleMetaBean = {
    MapMetaBean[ScheduleMetaBean](this ++ (FSchedule, FStart → asString(timeAttrs.start), FEnd → asString(timeAttrs.end)))
  }

  def schedule: MapBean = this.get[MapBean](FSchedule).getOrElse(MapBean.empty)

  def isContinuous: Boolean = this.get[String](FSchedule, FDeadline).exists(_.contains("fromNow"))

  def isTimerRoller: Boolean = this.get[Duration](FSchedule, FPeriod).isDefined

  def isPageRoller: Boolean = this.get[Int](FSchedule, FPage).isDefined

  def page: Int = schedule.get[Int](FPage).getOrElse(0)

  def pageSize: Int = schedule.get[Int](FPageSize).getOrElse(0)

  def maxPage: Int = schedule.get[Int](FMaxPage).getOrElse(0)

  def offset: Int = schedule.get[Int](FOffset).getOrElse(0)

  def deadlineOption: Option[String] = schedule.get[String](FDeadline)

  def start: LocalDateTime = schedule.get[LocalDateTime](FStart).getOrElse(LocalDateTime.MIN)

  def end: LocalDateTime = schedule.get[LocalDateTime](FEnd).getOrElse(LocalDateTime.MIN)

  def period: Duration = schedule.get[Duration](FPeriod).getOrElse(Duration.Undefined)

  def maxPeriod: Duration = schedule.get[Duration](FMaxPeriod).getOrElse(period)
}

object ScheduleActorPublisherMessage {

  sealed trait Action

  case object ClientInit extends Action

  case object NextPage extends Action

  case object NextTime extends Action

  case object Deliver extends Action

  case object Grab extends Action

  case class PageAttrs(page: Int, pageSize: Int, maxPage: Int, offset: Int)

  case class TimeAttrs(start: LocalDateTime, end: LocalDateTime, period: Duration, maxPeriod: Duration, deadline: () ⇒ LocalDateTime)

  case class ScheduleSetting(
                              scheduleMetaBean: ScheduleMetaBean,
                              pageAttrs: Option[PageAttrs],
                              timeAttrs: Option[TimeAttrs],
                              isContinuous: Boolean
                            ) {
    def updated(): ScheduleSetting = {
      val _scheduleMetaBean = pageAttrs.map(scheduleMetaBean.update)
        .flatMap(metaBean ⇒ timeAttrs.map(MapMetaBean[ScheduleMetaBean](metaBean).update))
        .getOrElse(scheduleMetaBean)
      this.copy(scheduleMetaBean = _scheduleMetaBean)
    }

    def updateTime(start: LocalDateTime, end: LocalDateTime): Unit = {
      this.copy(timeAttrs = timeAttrs.map(_.copy(start = start, end = end)))
    }

    def updatePage(page: Int): Unit = {
      this.copy(pageAttrs = pageAttrs.map(_.copy(page = page)))
    }
  }

  sealed trait Direction

  case object Up extends Direction

  case object Down extends Direction

  class DrillStack(actions: Seq[Action], var count: Int = 0, var totalCount: Int = 0) {
    private var _direction: Direction = Down
    private var idx = 0

    def current: Action = actions(idx)

    def direction: Direction = _direction

    def first: Boolean = idx == 0

    def last: Boolean = idx == actions.length - 1

    def drill(direction: Direction): Action = {
      _direction = direction
      direction match {
        case Up ⇒ idx = idx - 1; current
        case Down ⇒ idx = idx + 1; current
      }
    }
  }

  object ScheduleSetting {
    def apply(config: SourceMetaBean): ScheduleSetting = {
      val scheduleConfig = config.mapTo[ScheduleMetaBean]
      val timeSetting = if (scheduleConfig.isTimerRoller) {
        val deadline: () ⇒ LocalDateTime = scheduleConfig.deadlineOption match {
          case Some("") | None ⇒ throw new IllegalArgumentException(s"deadline is required, $scheduleConfig")
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
        }
        TimeAttrs(
          start = scheduleConfig.start,
          end = scheduleConfig.end,
          period = scheduleConfig.period,
          maxPeriod = scheduleConfig.maxPeriod,
          deadline = deadline
        )
      } else {
        null
      }
      val pageSetting = if (scheduleConfig.isPageRoller) {
        PageAttrs(
          page = scheduleConfig.page,
          pageSize = scheduleConfig.pageSize,
          maxPage = scheduleConfig.maxPage,
          offset = scheduleConfig.offset
        )
      } else {
        null
      }
      ScheduleSetting(
        scheduleMetaBean = scheduleConfig,
        pageAttrs = Option(pageSetting),
        timeAttrs = Option(timeSetting),
        isContinuous = scheduleConfig.isContinuous
      )
    }
  }

}

trait ScheduleActorPublisher[T, A]
  extends ActorPublisher[TeleporterMessage[T]]
    with Component
    with LazyLogging {

  private implicit val _center: TeleporterCenter = center

  import ScheduleActorPublisherMessage._

  implicit val executionContext: ExecutionContext
  protected val transaction: Transaction[T, SourceMetaBean] = Transaction[T, SourceMetaBean](key, center.defaultRecoveryPoint)
  protected var client: A = _
  protected var drillStack: DrillStack = _
  private var scheduleSetting: ScheduleSetting = _
  protected var sourceContext: SourceContext = _
  private var _iterator: Iterator[T] = _
  protected var enforcer: Enforcer = _
  val counter: MetricsCounter = center.metricsRegistry.counter(Measurement(key, Seq(Tags.success)))

  import transaction._

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    self ! ClientInit
  }

  override def receive: Receive = ({
    case ClientInit ⇒
      try {
        sourceContext = center.context.getContext[SourceContext](key)
        enforcer = Enforcer(key, sourceContext.config)
        client = center.components.address[A](sourceContext.config.address)
        scheduleSetting = ScheduleSetting(sourceContext.config)
        val drillActions = Seq.newBuilder[Action]
        if (scheduleSetting.timeAttrs.isDefined) drillActions += NextTime
        if (scheduleSetting.pageAttrs.isDefined) drillActions += NextPage
        drillActions += Grab
        drillStack = new DrillStack(drillActions.result())
        self ! drillStack.current
      } catch {
        case e: Exception ⇒ enforcer.execute(e)
      }
    case Request(n) ⇒ if (totalDemand == n && _iterator != null && drillStack != null && drillStack.last) {
      self ! Deliver
    }
    case tId: TId ⇒ end(tId)
    case Cancel ⇒
      center.context.getContext[SourceContext](key).address().clientRefs.close(key)
      context.stop(self)
  }: Receive).orElse(drill)

  protected def drill: Receive = {
    case NextTime ⇒
      val timeAttrs = scheduleSetting.timeAttrs.get
      val start = if (timeAttrs.end == LocalDateTime.MIN) timeAttrs.start else timeAttrs.end
      val distance = JDuration.between(start, timeAttrs.deadline()).toNanos
      if ((scheduleSetting.isContinuous && distance > timeAttrs.period.toNanos)
        || (!scheduleSetting.isContinuous && distance > 0)) {
        val nanos = distance min timeAttrs.maxPeriod.toNanos min timeAttrs.period.toNanos
        val end = start.plusNanos(nanos)
        scheduleSetting.updateTime(start, end)
        self ! drillStack.drill(Down)
      } else if (scheduleSetting.isContinuous) {
        context.system.scheduler.scheduleOnce(timeAttrs.period.asInstanceOf[FiniteDuration], self, drillStack.current)
      } else {
        doComplete()
      }
    case NextPage ⇒
      val pageAttrs = scheduleSetting.pageAttrs.get
      drillStack.direction match {
        case Up ⇒
          if (drillStack.count < pageAttrs.pageSize || pageAttrs.page > pageAttrs.maxPage) {
            if (drillStack.first) {
              doComplete()
            } else {
              scheduleSetting.updatePage(0)
              self ! drillStack.drill(Up)
            }
          } else {
            val page = pageAttrs.page + 1
            scheduleSetting.updatePage(page)
            self ! drillStack.drill(Down)
          }
        case Down ⇒
          self ! drillStack.drill(Down)
      }
    case Grab ⇒
      _grab()
    case Deliver ⇒
      deliver()
  }

  protected def _grab(): Unit = {
    drillStack.count = 0
    grab(scheduleSetting.updated()).onComplete {
      case Success(it) ⇒
        _iterator = it
        self ! Deliver
      case Failure(e) ⇒ enforcer.execute(e)
    }
  }

  protected def grab(config: ScheduleSetting): Future[Iterator[T]]

  var cancellable: Cancellable = _

  def doComplete(): Unit = {
    if (!scheduleSetting.isContinuous) {
      if (!isComplete) {
        cancellable = context.system.scheduler.scheduleOnce(5.seconds, self, Deliver)
      } else {
        if (cancellable != null && !cancellable.isCancelled) cancellable.cancel()
        center.context.getContext[SourceContext](key).address().clientRefs.close(key)
        onCompleteThenStop()
      }
    }
  }

  @tailrec
  final def deliver(): Unit = {
    if (totalDemand > 0 && isActive) {
      tryBegin(scheduleSetting.scheduleMetaBean, {
        drillStack.count += 1
        drillStack.totalCount += 1
        Component.getIfPresent(_iterator)
      }, onNext) match {
        case Transaction.Normal | Transaction.Retry ⇒
          counter.inc()
          deliver()
        case Transaction.NoData ⇒
          if (drillStack.first) {
            doComplete()
          } else {
            self ! drillStack.drill(Up)
          }
        case Transaction.OverLimit ⇒
          context.system.scheduler.scheduleOnce(1.seconds, self, Deliver)
      }
    }
  }
}
