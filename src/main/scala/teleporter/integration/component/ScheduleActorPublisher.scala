package teleporter.integration.component

import java.time.{LocalDateTime, Duration ⇒ JDuration}

import akka.actor.Cancellable
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.core.TeleporterConfig._
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics.{Measurement, Tags}
import teleporter.integration.transaction.Transaction
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{Dates, MapBean, MapMetadata}

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by kui.dai on 2016/7/11.
  */
trait ScheduleMetadata extends MapMetadata {
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

  def update(config: MapBean, rollPage: RollPage): MapBean = config ++ (FSchedule, FPage → rollPage.currPage)

  def update(config: MapBean, rollTime: RollTime): MapBean = config ++ (FSchedule, FStart → rollTime.start, FEnd → rollTime.end)

  def isContinuous()(implicit config: MapBean) = config.__dict__[String](FSchedule, FDeadline).exists(_.contains("fromNow"))

  def isTimerRoller()(implicit config: MapBean) = config.__dict__[Duration](FSchedule, FPeriod).isDefined

  def isPageRoller()(implicit config: MapBean) = config.__dict__[Int](FSchedule, FPage).isDefined
}

object ScheduleActorPublisherMessage extends ScheduleMetadata {

  sealed trait Action

  case object ClientInit extends Action

  case object NextPage extends Action

  case object NextTime extends Action

  case object Deliver extends Action

  case object Grab extends Action

  case class PageAttrs(page: Int, pageSize: Int, maxPage: Int, offset: Int)

  case class TimeAttrs(start: LocalDateTime, end: LocalDateTime, period: Duration, maxPeriod: Duration, deadline: () ⇒ LocalDateTime)

  case class ScheduleSetting(
                              pageAttrs: Option[PageAttrs],
                              timeAttrs: Option[TimeAttrs],
                              isContinuous: Boolean
                            ) {
    def merge(config: SourceConfig): SourceConfig = {
      config ++ (FSchedule, pageAttrs.map(p ⇒ Array(FPage → asString(p.page), FOffset → asString(p.page * p.pageSize)))
        .getOrElse(timeAttrs.map(t ⇒ Array(FStart → asString(t.start), FEnd → asString(t.end))).getOrElse(Array.empty)): _*)
    }

    def updated(start: LocalDateTime, end: LocalDateTime): ScheduleSetting = {
      this.copy(timeAttrs = timeAttrs.map(_.copy(start = start, end = end)))
    }

    def updated(page: Int): ScheduleSetting = {
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
    def apply(config: MapBean): ScheduleSetting = {
      val scheduleConfig = config[MapBean](FSchedule)
      val _isContinuous = isContinuous()(config)
      val _isTimeRoller = isTimerRoller()(config)
      val _isPageRoller = isPageRoller()(config)
      val timeSetting = _isTimeRoller match {
        case true ⇒
          val deadline: () ⇒ LocalDateTime = scheduleConfig.__dict__[String](FDeadline) match {
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
          val period = scheduleConfig.__dict__[Duration](FPeriod).getOrElse(Duration.Undefined)
          TimeAttrs(
            start = scheduleConfig.__dict__[LocalDateTime](FStart).getOrElse(LocalDateTime.MIN),
            end = scheduleConfig.__dict__[LocalDateTime](FEnd).getOrElse(LocalDateTime.MIN),
            period = period,
            maxPeriod = scheduleConfig.__dict__[Duration](FMaxPeriod).getOrElse(period),
            deadline = deadline
          )
        case false ⇒ null
      }
      val pageSetting = _isPageRoller match {
        case true ⇒
          PageAttrs(
            page = scheduleConfig.__dict__[Int](FPage).getOrElse(0),
            pageSize = scheduleConfig.__dict__[Int](FPageSize).getOrElse(0),
            maxPage = scheduleConfig.__dict__[Int](FMaxPage).getOrElse(0),
            offset = scheduleConfig.__dict__[Int](FOffset).getOrElse(0)
          )
        case false ⇒ null
      }
      ScheduleSetting(
        pageAttrs = Option(pageSetting),
        timeAttrs = Option(timeSetting),
        isContinuous = _isContinuous
      )
    }
  }

}


trait ScheduleActorPublisher[T, A]
  extends ActorPublisher[TeleporterMessage[T]]
    with Component with SourceMetadata with ScheduleMetadata
    with LazyLogging {

  private implicit val _center: TeleporterCenter = center

  import ScheduleActorPublisherMessage._

  implicit val executionContext: ExecutionContext
  protected val transaction = Transaction[T, SourceConfig](key, center.defaultRecoveryPoint)
  protected var client: A = _
  protected var drillStack: DrillStack = _
  private var scheduleSetting: ScheduleSetting = _
  private var config: SourceConfig = _
  private var _iterator: Iterator[T] = _
  protected var enforcer: Enforcer = _
  val counter = center.metricsRegistry.counter(Measurement(key, Seq(Tags.success)))

  import transaction._

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    self ! ClientInit
  }

  override def receive: Receive = ({
    case ClientInit ⇒
      try {
        val sourceContext = center.context.getContext[SourceContext](key)
        config = sourceContext.config
        enforcer = Enforcer(key, config)
        client = center.components.address[A](sourceContext.addressKey)
        scheduleSetting = ScheduleSetting(config)
        config = scheduleSetting.merge(config)
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
        scheduleSetting = scheduleSetting.updated(start, end)
        config = scheduleSetting.merge(config)
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
              scheduleSetting = scheduleSetting.updated(0)
              self ! drillStack.drill(Up)
            }
          } else {
            val page = pageAttrs.page + 1
            scheduleSetting = scheduleSetting.updated(page)
            config = scheduleSetting.merge(config)
            self ! drillStack.drill(Down)
          }
        case Down ⇒
          self ! drillStack.drill(Down)
      }
    case Grab ⇒
      _grab(config)
    case Deliver ⇒
      deliver()
  }

  protected def _grab(config: MapBean): Unit = {
    drillStack.count = 0
    grab(config).onComplete {
      case Success(it) ⇒
        _iterator = it
        self ! Deliver
      case Failure(e) ⇒ enforcer.execute(e)
    }
  }

  protected def grab(config: MapBean): Future[Iterator[T]]

  var cancellable: Cancellable = _

  def doComplete() = {
    if (!scheduleSetting.isContinuous) {
      if (!isComplete()) {
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
      tryBegin(config, {
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
          context.system.scheduler.scheduleOnce(5.seconds, self, Deliver)
      }
    }
  }
}
