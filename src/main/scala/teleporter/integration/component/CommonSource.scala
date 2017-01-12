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

  private def closeStage(): Unit =
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

  override def supervisionStrategy: TeleporterAttribute.SupervisionStrategy = inheritedAttributes.get[TeleporterAttribute.SupervisionStrategy].getOrElse(TeleporterAttribute.emptySupervisionStrategy)

  override def teleporterFailure(ex: Throwable): Unit = {
    retries += 1
    matchRule(ex).foreach {
      rule ⇒
        if (retries < rule.directive.retries) {
          rule.directive.delay match {
            case Duration.Zero ⇒ decide(ex, rule.directive)
            case d: FiniteDuration ⇒ scheduleOnce((ex, rule.directive), d)
            case _ ⇒
          }
        } else {
          rule.directive.next.foreach(decide(ex, _))
        }
    }
  }

  override def reload(ex: Throwable): Unit = {
    close(client)
    preStart()
  }

  override def retry(ex: Throwable): Unit = onPull()

  override def resume(ex: Throwable): Unit = onPull()

  override def stop(ex: Throwable): Unit = failStage(ex)

  setHandler(shape.out, this)
}

abstract class CommonSource[C, Out](name: String) extends GraphStage[SourceShape[Out]] {
  override protected def initialAttributes: Attributes = Attributes.name(name)

  override def shape: SourceShape[Out] = SourceShape(Outlet[Out](s"$name.out"))

  override val toString: String = name

  val out: Outlet[Out] = shape.out

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

    def nextPage(): RollerContext = {
      require(this.pagination.isDefined, "Pagination must support before call")
      val pagination = this.pagination.get
      val nextPagination = pagination.copy(page = pagination.page + 1)
      this.copy(pagination = Some(nextPagination), state = Paging)
    }

    def firstPage(): RollerContext = {
      require(this.pagination.isDefined, "Pagination must support before call")
      val pagination = this.pagination.get
      val firstPagination = pagination.copy(page = 1)
      this.copy(pagination = Some(firstPagination), state = Paging)
    }

    def nextTimeline(): RollerContext = {
      require(this.timeline.isDefined, "Pagination must support before call")
      val timeline = this.timeline.get
      val distance = java.time.Duration.between(timeline.start, timeline.deadline()).toNanos
      val step = distance min timeline.maxPeriod.toNanos min timeline.period.toNanos
      val nextTimeline = timeline.copy(start = timeline.start, end = timeline.start.plusNanos(step))
      this.copy(timeline = Some(nextTimeline), state = Timing)
    }

    def timeNear: Boolean = {
      require(this.forever, "Must forever roller")
      val timeline = this.timeline.get
      java.time.Duration.between(timeline.start, timeline.deadline()).toNanos < timeline.period.toNanos
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
      val schedule = config.get[MapBean](FRoller).getOrElse(MapBean.empty)
      val pagination = schedule.get[Int](FPage).map { page ⇒
        Pagination(
          page = page,
          pageSize = schedule.get[Int](FPageSize).getOrElse(20),
          maxPage = schedule.get[Int](FMaxPage).getOrElse(Int.MaxValue)
        )
      }
      val timeline = schedule.get[Duration](FPeriod).map { period ⇒
        Timeline(
          start = schedule[LocalDateTime](FStart),
          end = schedule[LocalDateTime](FEnd),
          period = period,
          maxPeriod = schedule.get[Duration](FMaxPeriod).getOrElse(period),
          deadline = schedule.get[String](FDeadline) match {
            case Some("") | None ⇒ throw new IllegalArgumentException(s"deadline is required, $schedule")
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
      RollerContext(pagination = pagination,
        timeline = timeline,
        forever = schedule.get[String](FDeadline).exists(_.contains("fromNow")),
        state = Paging
      )
    }
  }

}

abstract class RollerSource[C, Out](name: String, rollerContext: RollerContext) extends CommonSource[C, Out](name) {

  import Roller._

  var currRollerContext: RollerContext = rollerContext

  @tailrec
  final override def readData(client: C): Option[Out] = {
    val opt = readData(client, currRollerContext)
    opt match {
      case None ⇒
        currRollerContext.state match {
          case Paging ⇒
            if (currRollerContext.timeline.isDefined && !(currRollerContext.forever && currRollerContext.timeNear)) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
              readData(client)
            } else {
              opt
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
            readData(client)
        }
      case Some(_) ⇒
        currRollerContext.state match {
          case Paging ⇒ currRollerContext = currRollerContext.nextPage()
          case Timing ⇒
            if (currRollerContext.pagination.isEmpty && !(currRollerContext.forever && currRollerContext.timeNear)) {
              currRollerContext = currRollerContext.nextTimeline()
            }
        }
        opt
    }
  }

  def readData(client: C, rollerContext: RollerContext): Option[Out]

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new CommonSourceGraphStageLogic(shape, create, readData, close, inheritedAttributes) {
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
          if (!currRollerContext.forever) {
            completeStage()
          } else {
            scheduleOnce('pull, currRollerContext.timeline.get.period.asInstanceOf[FiniteDuration] / 2)
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

  implicit val executeContext: ExecutionContextExecutor = {
    val mater = materializer.asInstanceOf[ActorMaterializer]
    val dispatcherAttr = inheritedAttributes.getAttribute(classOf[TeleporterAttribute.Dispatcher])
    if (dispatcherAttr.isPresent) {
      mater.system.dispatchers.lookup(dispatcherAttr.get().dispatcher)
    } else {
      mater.executionContext
    }
  }
  var retries = 0
  var client: S = _
  //  implicit val context = ExecutionContexts.sameThreadExecutionContext
  var resource: Promise[S] = Promise[S]()
  setHandler(shape.out, this)

  override def preStart(): Unit = createStream(false)

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

  private def closeAndThen(f: () ⇒ Unit): Unit = {
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

  override def supervisionStrategy: TeleporterAttribute.SupervisionStrategy = inheritedAttributes.get[TeleporterAttribute.SupervisionStrategy].getOrElse(TeleporterAttribute.emptySupervisionStrategy)

  @scala.throws[Exception](classOf[Exception])
  override protected def onTimer(timerKey: Any): Unit = {
    timerKey match {
      case (ex: Throwable, d: integration.supervision.Supervision.Directive) ⇒ decide(ex, d)
      case _ ⇒ log.warning(s"Unmatched $timerKey")
    }
  }

  override def teleporterFailure(ex: Throwable): Unit = {
    retries += 1
    ex match {
      case NonFatal(_) ⇒
        matchRule(ex).foreach {
          rule ⇒
            if (retries < rule.directive.retries) {
              rule.directive.delay match {
                case Duration.Zero ⇒ decide(ex, rule.directive)
                case d: FiniteDuration ⇒ scheduleOnce(rule.directive, d)
                case _ ⇒
              }
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
    onResourceReady(close(_, executeContext))
    failStage(ex)
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

abstract class RollerSourceAsync[T, S](name: String, rollerContext: RollerContext)
  extends CommonSourceAsync[T, S](name) {

  import Roller._

  var currRollerContext: RollerContext = rollerContext

  override def readData(client: S, executionContext: ExecutionContext): Future[Option[T]] = {
    implicit val ec = executionContext
    val opt = readData(client, currRollerContext, executionContext)
    opt flatMap {
      case None ⇒
        currRollerContext.state match {
          case Paging ⇒
            if (currRollerContext.timeline.isDefined && !(currRollerContext.forever && currRollerContext.timeNear)) {
              currRollerContext = currRollerContext.nextTimeline()
              currRollerContext = currRollerContext.firstPage()
              readData(client, ec)
            } else {
              opt
            }
          case Timing ⇒
            currRollerContext = currRollerContext.nextTimeline()
            readData(client, ec)
        }
      case Some(_) ⇒
        currRollerContext.state match {
          case Paging ⇒ currRollerContext = currRollerContext.nextPage()
          case Timing ⇒
            if (currRollerContext.pagination.isEmpty && !(currRollerContext.forever && currRollerContext.timeNear)) {
              currRollerContext = currRollerContext.nextTimeline()
            }
        }
        opt
    }
  }

  def readData(client: S, rollerContext: RollerContext, executionContext: ExecutionContext): Future[Option[T]]

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new CommonSourceAsyncGraphStageLogic[T, S](shape, create, readData, close, inheritedAttributes) {
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
            completeStage()
          } else {
            scheduleOnce('pull, currRollerContext.timeline.get.period.asInstanceOf[FiniteDuration] / 2)
          }
      }
      case scala.util.Failure(ex) ⇒ teleporterFailure(ex)
    }.invoke _
  }
}