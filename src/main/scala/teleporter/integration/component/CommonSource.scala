package teleporter.integration.component

import java.time.LocalDateTime

import akka.Done
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage._
import teleporter.integration
import teleporter.integration.component.Roller.RollerContext
import teleporter.integration.supervision.SourceDecider
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{Dates, MapBean}

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Created by huanwuji 
  * date 2017/1/4.
  */
class CommonSourceGraphStageLogic[C, Out](shape: SourceShape[Out],
                                          create: () ⇒ C,
                                          readData: (C) ⇒ Option[Out],
                                          close: (C) ⇒ Unit,
                                          inheritedAttributes: Attributes)
  extends TimerGraphStageLogic(shape) with SourceDecider with OutHandler with StageLogging {
  var retries = 0
  var client: C = _

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    try {
      client = create()
    } catch {
      case NonFatal(ex) ⇒ teleporterFailure(ex)
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def onPull(): Unit = {
    try {
      pushData()
      retries = 0
    } catch {
      case NonFatal(ex) ⇒ teleporterFailure(ex)
    }
  }

  protected def pushData(): Unit = {
    readData(client) match {
      case None ⇒ closeStage()
      case Some(elem) ⇒ push(shape.out, elem)
    }
  }


  @scala.throws[Exception](classOf[Exception])
  override def onDownstreamFinish(): Unit = closeStage()

  protected def closeStage(): Unit =
    try {
      close(client)
      completeStage()
    } catch {
      case NonFatal(ex) ⇒ failStage(ex)
    }

  @scala.throws[Exception](classOf[Exception])
  override protected def onTimer(timerKey: Any): Unit = {
    timerKey match {
      case (ex: Throwable, d: integration.supervision.Supervision.Directive) ⇒ decide(ex, d)
      case _ ⇒ log.warning(s"Unmatched $timerKey")
    }
  }

  override def supervisionStrategy: TeleporterAttributes.SupervisionStrategy = inheritedAttributes.get[TeleporterAttributes.SupervisionStrategy].getOrElse(TeleporterAttributes.emptySupervisionStrategy)

  override def teleporterFailure(ex: Throwable): Unit = {
    super.teleporterFailure(ex)
    matchRule(ex).foreach {
      rule ⇒
        if (retries < rule.directive.retries) {
          rule.directive.delay match {
            case Duration.Zero ⇒ decide(ex, rule.directive)
            case d: FiniteDuration ⇒ scheduleOnce((ex, rule.directive), d)
            case _ ⇒
          }
          retries += 1
        } else {
          rule.directive.next.foreach(decide(ex, _))
        }
    }
  }

  override def reload(ex: Throwable): Unit = {
    close(client)
    preStart()
    retry(ex)
  }

  override def retry(ex: Throwable): Unit = onPull()

  override def resume(ex: Throwable): Unit = onPull()

  override def stop(ex: Throwable): Unit = {
    close(client)
    failStage(ex)
  }

  setHandler(shape.out, this)
}

abstract class CommonSource[C, Out](name: String) extends GraphStage[SourceShape[Out]] {
  override protected def initialAttributes: Attributes = Attributes.name(name)

  val out: Outlet[Out] = Outlet[Out](s"$name.out")

  override def shape: SourceShape[Out] = SourceShape(out)

  override val toString: String = name

  def create(): C

  def readData(client: C): Option[Out]

  def close(client: C): Unit

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new CommonSourceGraphStageLogic(shape, create, readData, close, inheritedAttributes)
}

object RollerMetaBean {
  val FRoller = "roller"
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

object Roller {

  sealed trait State

  case object Normal extends State

  case object Paging extends State

  case object Timing extends State

  case class Pagination(page: Int, pageSize: Int, maxPage: Int) {
    def offset: Long = page * pageSize
  }

  case class Timeline(start: LocalDateTime, end: LocalDateTime, period: Duration, maxPeriod: Duration, deadline: () ⇒ LocalDateTime)

  case class RollerContext(pagination: Option[Pagination],
                           timeline: Option[Timeline],
                           forever: Boolean, state: State) {

    import RollerMetaBean._

    def canPaging: Boolean = pagination.exists(p ⇒ p.page <= p.maxPage)

    def nextPage(): RollerContext = {
      require(this.pagination.isDefined, "Pagination must support before call")
      val pagination = this.pagination.get
      val nextPagination = pagination.copy(page = pagination.page + 1)
      this.copy(pagination = Some(nextPagination), state = Paging)
    }

    def onlyPage: Boolean = pagination.isDefined && timeline.isEmpty

    def onlyTimeline: Boolean = pagination.isEmpty && timeline.isDefined

    def isPageTimeline: Boolean = pagination.isDefined && timeline.isDefined

    def firstPage(): RollerContext = {
      require(this.pagination.isDefined, "Pagination must support before call")
      val pagination = this.pagination.get
      val firstPagination = pagination.copy(page = 1)
      this.copy(pagination = Some(firstPagination), state = Paging)
    }

    def nextTimeline(): RollerContext = {
      require(this.timeline.isDefined, "Timeline must support before call")
      val timeline = this.timeline.get
      val begin = if (timeline.end == null) timeline.start else timeline.end
      val distance = java.time.Duration.between(begin, timeline.deadline()).toNanos
      val step = if (forever) {
        (distance min timeline.maxPeriod.toNanos) min (distance max timeline.period.toNanos)
      } else {
        (distance min timeline.maxPeriod.toNanos) max (distance min timeline.period.toNanos)
      }
      val nextTimeline = timeline.copy(start = begin, end = begin.plusNanos(step))
      this.copy(timeline = Some(nextTimeline), state = Timing)
    }

    def checkStartTimeOver: Boolean = !this.forever && timeline.exists(t ⇒ t.deadline().isEqual(t.start))

    def checkStartTimeNear: Boolean = {
      this.forever && this.timeline.exists(t ⇒ java.time.Duration.between(t.end, t.deadline()).toNanos < t.period.toNanos)
    }

    def toMap: Map[String, Any] = {
      val builder = Map.newBuilder[String, Any]
      this.pagination.foreach {
        p ⇒ builder += (FPage → asString(p.page), FOffset → asString(p.page * p.pageSize))
      }
      this.timeline.foreach {
        t ⇒ builder += (FStart → asString(t.start), FEnd → asString(t.end))
      }
      builder.result()
    }
  }

  object RollerContext {

    import RollerMetaBean._

    def merge(config: MapBean, rollerContext: RollerContext): MapBean = {
      config ++ (FRoller, rollerContext.toMap.toSeq: _*)
    }

    def apply(config: MapBean): RollerContext = {
      val roller = config.get[MapBean](FRoller).getOrElse(MapBean.empty)
      val pagination = roller.get[Int](FPage).map { page ⇒
        Pagination(
          page = page,
          pageSize = roller.get[Int](FPageSize).getOrElse(20),
          maxPage = roller.get[Int](FMaxPage).getOrElse(Int.MaxValue)
        )
      }
      val timeline = roller.get[Duration](FPeriod).map { period ⇒
        Timeline(
          start = roller[LocalDateTime](FStart),
          end = roller.get[LocalDateTime](FEnd).orNull,
          period = period,
          maxPeriod = roller.get[Duration](FMaxPeriod).getOrElse(period),
          deadline = roller.get[String](FDeadline) match {
            case Some("") | None ⇒ throw new IllegalArgumentException(s"deadline is required, $roller")
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
          })
      }
      val state = if (pagination.isDefined) Paging else if (timeline.isDefined) Timing else Normal
      val context = RollerContext(
        pagination = pagination,
        timeline = timeline,
        forever = roller.get[String](FDeadline).exists(_.contains("fromNow")),
        state = state
      )
      if (context.timeline.exists(_.end == null)) context.nextTimeline().copy(state = context.state) else context
    }
  }

}

abstract class RollerSource[C, Out](name: String, rollerContext: RollerContext) extends CommonSource[C, Out](name) {

  import Roller._

  var currRollerContext: RollerContext = rollerContext
  var nextContinued = true

  override def readData(client: C): Option[Out] = {
    if (currRollerContext.forever) {
      foreverReadData(client)
    } else {
      beOverReadData(client)
    }
  }

  @tailrec
  final def foreverReadData(client: C): Option[Out] = {
    val data = if (currRollerContext.checkStartTimeNear) {
      return None
    } else {
      readData(client, currRollerContext)
    }
    data match {
      case None ⇒
        currRollerContext.state match {
          case Normal ⇒ None
          case Paging ⇒
            currRollerContext = currRollerContext.nextTimeline()
            currRollerContext = currRollerContext.firstPage()
            if (currRollerContext.checkStartTimeNear) {
              None
            } else {
              foreverReadData(client)
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
            if (!currRollerContext.checkStartTimeNear) {
              foreverReadData(client)
            } else None
        }
      case Some(_) ⇒
        currRollerContext.state match {
          case Normal ⇒
          case Paging ⇒
            currRollerContext = currRollerContext.nextPage()
            if (currRollerContext.isPageTimeline && !currRollerContext.canPaging) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
        }
        data
    }
  }

  @tailrec
  final def beOverReadData(client: C): Option[Out] = {
    val data = if (currRollerContext.checkStartTimeOver || (currRollerContext.onlyPage && !currRollerContext.canPaging)) {
      return None
    } else readData(client, currRollerContext)
    data match {
      case None ⇒
        currRollerContext.state match {
          case Normal ⇒ None
          case Paging ⇒
            if (!currRollerContext.checkStartTimeOver) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
              beOverReadData(client)
            } else None
          case Timing ⇒
            if (!currRollerContext.checkStartTimeOver) {
              currRollerContext = currRollerContext.nextTimeline()
              beOverReadData(client)
            } else None
        }
      case Some(_) ⇒
        currRollerContext.state match {
          case Normal ⇒
          case Paging ⇒
            currRollerContext = currRollerContext.nextPage()
            if (currRollerContext.isPageTimeline && !currRollerContext.canPaging) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
        }
        data
    }
  }

  def readData(client: C, rollerContext: RollerContext): Option[Out]

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new CommonSourceGraphStageLogic(shape, create, readData, close, inheritedAttributes) {
      @scala.throws[Exception](classOf[Exception])
      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case 'pull ⇒ onPull()
          case _ ⇒ super.onTimer(timerKey)
        }
      }

      override protected def pushData(): Unit = {
        readData(client) match {
          case None ⇒
            if (currRollerContext.forever) {
              scheduleOnce('pull, currRollerContext.timeline.get.period.asInstanceOf[FiniteDuration] / 2)
            } else {
              closeStage()
            }
          case Some(elem) ⇒ push(shape.out, elem)
        }
      }
    }
}

class CommonSourceAsyncGraphStageLogic[T, S](shape: SourceShape[T],
                                             create: ExecutionContext ⇒ Future[S],
                                             readData: (S, ExecutionContext) ⇒ Future[Option[T]],
                                             close: (S, ExecutionContext) ⇒ Future[Done],
                                             inheritedAttributes: Attributes)
  extends TimerGraphStageLogic(shape) with SourceDecider with OutHandler with StageLogging {

  implicit var executeContext: ExecutionContextExecutor = _
  var retries = 0
  var client: S = _
  var resource: Promise[S] = Promise[S]()
  setHandler(shape.out, this)

  override def preStart(): Unit = {
    executeContext = {
      val mater = materializer.asInstanceOf[ActorMaterializer]
      val dispatcherAttr = inheritedAttributes.getAttribute(classOf[TeleporterAttributes.Dispatcher])
      if (dispatcherAttr.isPresent) {
        mater.system.dispatchers.lookup(dispatcherAttr.get().dispatcher)
      } else {
        mater.executionContext
      }
    }
    createStream(false)
  }

  private def createStream(withPull: Boolean): Unit = {
    val cb = getAsyncCallback[Try[S]] {
      case scala.util.Success(res) ⇒
        resource.success(res)
        if (withPull) onPull()
      case scala.util.Failure(t) ⇒ teleporterFailure(t)
    }
    try {
      create(executeContext).onComplete(cb.invoke)
    } catch {
      case NonFatal(ex) ⇒ teleporterFailure(ex)
    }
  }

  private def onResourceReady(f: (S) ⇒ Unit): Unit = resource.future.onSuccess {
    case _resource ⇒ f(_resource)
  }

  val callback: (Try[Option[T]]) ⇒ Unit = getAsyncCallback[Try[Option[T]]] {
    case scala.util.Success(data) ⇒ data match {
      case Some(d) ⇒ push(shape.out, d)
      case None ⇒ closeStage()
    }
    case scala.util.Failure(ex) ⇒ teleporterFailure(ex)
  }.invoke _

  final override def onPull(): Unit = onResourceReady {
    _resource ⇒
      try {
        readData(_resource, executeContext).onComplete(callback)
      } catch {
        case e: Throwable ⇒ teleporterFailure(e)
      }
  }

  override def onDownstreamFinish(): Unit = closeStage()

  protected def closeAndThen(f: () ⇒ Unit): Unit = {
    setKeepGoing(true)
    val cb = getAsyncCallback[Try[Done]] {
      case scala.util.Success(_) ⇒ f()
      case scala.util.Failure(t) ⇒ failStage(t)
    }
    onResourceReady(res ⇒
      try {
        close(res, executeContext).onComplete(cb.invoke)
      } catch {
        case NonFatal(ex) ⇒ failStage(ex)
      })
  }

  private def restartState(): Unit = closeAndThen(() ⇒ {
    resource = Promise[S]()
    createStream(true)
  })

  private def closeStage(): Unit = closeAndThen(completeStage)

  override def supervisionStrategy: TeleporterAttributes.SupervisionStrategy = inheritedAttributes.get[TeleporterAttributes.SupervisionStrategy].getOrElse(TeleporterAttributes.emptySupervisionStrategy)

  @scala.throws[Exception](classOf[Exception])
  override protected def onTimer(timerKey: Any): Unit = {
    timerKey match {
      case (ex: Throwable, d: integration.supervision.Supervision.Directive) ⇒ decide(ex, d)
      case _ ⇒ log.warning(s"Unmatched $timerKey")
    }
  }

  override def teleporterFailure(ex: Throwable): Unit = {
    super.teleporterFailure(ex)
    ex match {
      case NonFatal(_) ⇒
        matchRule(ex).foreach {
          rule ⇒
            if (retries < rule.directive.retries) {
              rule.directive.delay match {
                case Duration.Zero ⇒ decide(ex, rule.directive)
                case d: FiniteDuration ⇒ scheduleOnce((ex, rule.directive), d)
                case _ ⇒
              }
              retries += 1
            } else {
              rule.directive.next.foreach(decide(ex, _))
            }
        }
    }
  }

  override def reload(ex: Throwable): Unit = restartState()

  override def retry(ex: Throwable): Unit = onPull()

  override def resume(ex: Throwable): Unit = onPull()

  override def stop(ex: Throwable): Unit = {
    closeAndThen(() ⇒ failStage(ex))
  }
}

abstract class CommonSourceAsync[T, S](name: String) extends GraphStage[SourceShape[T]] {
  val out: Outlet[T] = Outlet[T](s"$name.out")
  override val shape = SourceShape(out)

  override def initialAttributes: Attributes = DefaultAttributes.unfoldResourceSourceAsync

  def create(ec: ExecutionContext): Future[S]

  def readData(client: S, ec: ExecutionContext): Future[Option[T]]

  def close(client: S, ec: ExecutionContext): Future[Done]

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new CommonSourceAsyncGraphStageLogic(shape, create, readData, close, inheritedAttributes)

  override def toString: String = name
}

abstract class RollerSourceAsync[T, C](name: String, rollerContext: RollerContext)
  extends CommonSourceAsync[T, C](name) {

  import Roller._

  var currRollerContext: RollerContext = rollerContext

  override def readData(client: C, executionContext: ExecutionContext): Future[Option[T]] = {
    if (currRollerContext.forever) {
      foreverReadData(client)(executionContext)
    } else {
      beOverReadData(client)(executionContext)
    }
  }

  final def foreverReadData(client: C)(implicit executionContext: ExecutionContext): Future[Option[T]] = {
    val data = if (currRollerContext.checkStartTimeNear) {
      return Future.successful(None)
    } else {
      readData(client, currRollerContext, executionContext)
    }
    data.flatMap {
      case None ⇒
        currRollerContext.state match {
          case Normal ⇒ Future.successful(None)
          case Paging ⇒
            currRollerContext = currRollerContext.nextTimeline()
            currRollerContext = currRollerContext.firstPage()
            if (currRollerContext.checkStartTimeNear) {
              Future.successful(None)
            } else {
              foreverReadData(client)
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
            if (!currRollerContext.checkStartTimeNear) {
              foreverReadData(client)
            } else Future.successful(None)
        }
      case Some(_) ⇒
        currRollerContext.state match {
          case Normal ⇒
          case Paging ⇒
            currRollerContext = currRollerContext.nextPage()
            if (currRollerContext.isPageTimeline && !currRollerContext.canPaging) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
        }
        data
    }
  }

  final def beOverReadData(client: C)(implicit executionContext: ExecutionContext): Future[Option[T]] = {
    val data = if (currRollerContext.checkStartTimeOver || (currRollerContext.onlyPage && !currRollerContext.canPaging)) {
      return Future.successful(None)
    } else readData(client, currRollerContext, executionContext)
    data.flatMap {
      case None ⇒
        currRollerContext.state match {
          case Normal ⇒ Future.successful(None)
          case Paging ⇒
            if (!currRollerContext.checkStartTimeOver) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
              beOverReadData(client)
            } else Future.successful(None)
          case Timing ⇒
            if (!currRollerContext.checkStartTimeOver) {
              currRollerContext = currRollerContext.nextTimeline()
              beOverReadData(client)
            } else Future.successful(None)
        }
      case Some(_) ⇒
        currRollerContext.state match {
          case Normal ⇒
          case Paging ⇒
            currRollerContext = currRollerContext.nextPage()
            if (currRollerContext.isPageTimeline && !currRollerContext.canPaging) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
        }
        data
    }
  }

  def readData(client: C, rollerContext: RollerContext, executionContext: ExecutionContext): Future[Option[T]]

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new CommonSourceAsyncGraphStageLogic[T, C](shape, create, readData, close, inheritedAttributes) {
    @scala.throws[Exception](classOf[Exception])
    override protected def onTimer(timerKey: Any): Unit = {
      timerKey match {
        case 'pull ⇒ onPull()
        case _ ⇒ super.onTimer(timerKey)
      }
    }

    override val callback: (Try[Option[T]]) ⇒ Unit = getAsyncCallback[Try[Option[T]]] {
      case scala.util.Success(data) ⇒ data match {
        case Some(d) ⇒ push(shape.out, d)
        case None ⇒
          if (!currRollerContext.forever) {
            closeAndThen(completeStage)
          } else {
            scheduleOnce('pull, currRollerContext.timeline.get.period.asInstanceOf[FiniteDuration] / 2)
          }
      }
      case scala.util.Failure(ex) ⇒ teleporterFailure(ex)
    }.invoke _
  }
}