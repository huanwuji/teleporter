package teleporter.integration.component

import java.text.MessageFormat
import java.util.Date

import akka.actor.{ActorRef, Cancellable}
import akka.stream.ActorAttributes.Dispatcher
import akka.stream.TeleporterAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage._
import akka.{Done, NotUsed}
import com.google.common.collect.EvictingQueue
import teleporter.integration
import teleporter.integration.component.SinkRoller.SinkRollerSetting
import teleporter.integration.supervision.SinkDecider
import teleporter.integration.utils.MapBean
import teleporter.integration.utils.MapBean._

import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Created by huanwuji 
  * date 2016/12/29.
  */
object SinkRoller {

  case class SinkRollerSetting(cron: Option[String], size: Option[Long] = None)

  object SinkRollerMetaBean {
    val FRoller = "roller"
    val FCron = "cron"
    val FSize = "size"
  }

  object SinkRollerSetting {

    import SinkRollerMetaBean._

    def apply(config: MapBean): Option[SinkRollerSetting] = {
      config.get[MapBean](FRoller).map { rollerConfig ⇒
        SinkRollerSetting(
          cron = rollerConfig.get[String](FCron),
          size = rollerConfig.get[Long](FSize))
      }
    }
  }

  def flow[In, Out](
                     setting: SinkRollerSetting,
                     cronRef: ActorRef,
                     catchSize: In ⇒ Long,
                     catchField: In ⇒ String,
                     rollKeep: (In, String) ⇒ Out,
                     rollDo: String ⇒ Out
                   ): Flow[In, Out, NotUsed] = Flow.fromGraph(new SinkRollerGraph[In, Out](setting, cronRef, catchSize, catchField, rollKeep, rollDo))
}

case class CurrentRollerRecord(template: String, var size: Long, var index: Int, var date: Date) {
  @volatile var fieldCache: String = _

  def fieldRefresh(): CurrentRollerRecord = {
    fieldCache = MessageFormat.format(template, date, index.toString)
    this
  }
}

object CurrentRollerRecord {
  def apply(template: String): CurrentRollerRecord = {
    CurrentRollerRecord(template = template, size = 0, index = 0, date = new Date).fieldRefresh()
  }
}

final case class SinkRollerGraph[In, Out](
                                           setting: SinkRollerSetting,
                                           cronRef: ActorRef,
                                           catchSize: In ⇒ Long,
                                           catchField: In ⇒ String,
                                           rollKeep: (In, String) ⇒ Out,
                                           rollDo: String ⇒ Out
                                         ) extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In]("SinkRoller.in")
  val out: Outlet[Out] = Outlet[Out]("SinkRoller.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.map

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private def decider = inheritedAttributes.get[ActorAttributes.SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

      var cron: Option[Cancellable] = _
      val rollFieldCache: mutable.HashMap[String, CurrentRollerRecord] = mutable.HashMap[String, CurrentRollerRecord]()
      var sendEndSignal: AsyncCallback[Out] = getAsyncCallback(signal ⇒ emit(out, signal))

      override def preStart(): Unit = {
        cron = setting.cron.map { cron ⇒
          Cron.source(cronRef, cron, Done).to(Sink.foreach { _ ⇒
            val date = new Date()
            rollFieldCache.values.foreach { value ⇒
              sendEndSignal.invoke(rollDo(value.fieldCache))
              value.size = 0
              value.date = date
              value.index = 0
              value.fieldRefresh()
            }
          }).run()(materializer)
        }
      }

      private def sizeTrigger(field: String, incSize: Long): Unit = {
        setting.size.exists { size ⇒
          val record = rollFieldCache(field)
          record.size += incSize
          val isRoller = record.size > size
          if (isRoller) {
            sendEndSignal.invoke(rollDo(record.fieldCache))
            record.size = 0
            record.index += 1
            record.fieldRefresh()
          }
          isRoller
        }
      }

      override def onPush(): Unit = {
        try {
          val elem = grab(in)
          val field = catchField(elem)
          push(out, rollKeep(elem, rollFieldCache.getOrElseUpdate(field, CurrentRollerRecord(field)).fieldCache))
          setting.size.foreach(_ ⇒ sizeTrigger(field, catchSize(elem)))
        } catch {
          case NonFatal(ex) ⇒ decider(ex) match {
            case Supervision.Stop ⇒ failStage(ex)
            case _ ⇒ pull(in)
          }
        }
      }

      override def onPull(): Unit = if (!hasBeenPulled(in)) pull(in)

      override def postStop(): Unit = cron.foreach(_.cancel())

      setHandlers(in, out, this)
    }
}

abstract class CommonSink[C, In, Out](name: String) extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In](s"$name.in")
  val out: Outlet[Out] = Outlet[Out](s"$name.out")
  override val shape: FlowShape[In, Out] = FlowShape[In, Out](in, out)

  override def initialAttributes: Attributes = Attributes.name(name)

  override val toString: String = name

  def create(): C

  def write(client: C, elem: In): Out

  def close(client: C): Unit

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with SinkDecider with OutHandler with InHandler with StageLogging {
      var client: C = _
      var lastElem: Option[In] = _
      var retries: Int = _

      @scala.throws[Exception](classOf[Exception])
      override def preStart(): Unit = {
        createClient()
      }

      def createClient(): Unit = {
        try {
          client = create()
        } catch {
          case NonFatal(ex) ⇒ teleporterFailure(ex)
        }
      }

      override def onPush(): Unit = {
        writeData(grab(in))
      }

      def writeData(elem: In): Unit = {
        try {
          push(out, write(client, elem))
          retries = 0
          lastElem = None
        } catch {
          case NonFatal(ex) ⇒
            lastElem = Some(elem)
            teleporterFailure(ex)
        }
      }

      override def onPull(): Unit = pull(in)

      @scala.throws[Exception](classOf[Exception])
      override def onDownstreamFinish(): Unit = {
        closeAndThen(super.onDownstreamFinish)
      }

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFailure(ex: Throwable): Unit = {
        closeAndThen(() ⇒ super.onUpstreamFailure(ex))
      }

      override def onUpstreamFinish(): Unit = closeAndThen(completeStage)

      private def closeAndThen(f: () ⇒ Unit): Unit = {
        try {
          close(client)
          f()
        } catch {
          case NonFatal(ex1) ⇒ failStage(ex1)
        }
      }

      @scala.throws[Exception](classOf[Exception])
      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case (ex: Throwable, d: integration.supervision.Supervision.Directive) ⇒ decide(ex, d)
          case _ ⇒ log.warning(s"Unmatched timeKey: $timerKey")
        }
      }

      override def supervisionStrategy: SupervisionStrategy = inheritedAttributes.get[SupervisionStrategy].getOrElse(TeleporterAttributes.emptySupervisionStrategy)

      override def teleporterFailure(ex: Throwable): Unit = {
        super.teleporterFailure(ex)
        matchRule(ex) match {
          case Some(rule) ⇒
            if (rule.directive.retries == -1 || retries < rule.directive.retries) {
              rule.directive.delay match {
                case Duration.Zero ⇒ decide(ex, rule.directive)
                case d: FiniteDuration ⇒ scheduleOnce((ex, rule.directive), d)
                case _ ⇒
              }
              retries += 1
            } else {
              rule.directive.next.foreach(decide(ex, _))
            }
          case None ⇒ stop(ex)
        }
      }

      override def reload(ex: Throwable): Unit = {
        closeAndThen(() ⇒ {
          createClient()
          retry(ex)
        })
      }

      override def retry(ex: Throwable): Unit = lastElem match {
        case Some(elem) ⇒ writeData(elem)
        case None ⇒
      }

      override def resume(ex: Throwable): Unit = if (!isClosed(in)) pull(in)

      override def stop(ex: Throwable): Unit = {
        closeAndThen(() ⇒ failStage(ex))
      }

      setHandlers(in, out, this)
    }
}

abstract class CommonSinkAsyncUnordered[C, In, Out](name: String, parallelism: Int) extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In](s"$name.in")
  val out: Outlet[Out] = Outlet[Out](s"$name.out")
  override val shape: FlowShape[In, Out] = FlowShape[In, Out](in, out)

  override def initialAttributes: Attributes = Attributes.name(name)

  override val toString: String = name

  def create(ex: ExecutionContext): Future[C]

  def write(client: C, elem: In, executionContext: ExecutionContext): Future[Out]

  def close(client: C, ex: ExecutionContext): Future[Done]

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with SinkDecider with OutHandler with InHandler with StageLogging {
      implicit var executionContext: ExecutionContextExecutor = _
      private var inFlight = 0
      private var retriesBuffer: EvictingQueue[In] = _
      private var buffer: EvictingQueue[Out] = _
      var client: C = _
      var retries: Int = _

      private[this] def todo = inFlight + buffer.size()

      override def preStart(): Unit = {
        executionContext = {
          val mater = materializer.asInstanceOf[ActorMaterializer]
          val dispatcherAttr = inheritedAttributes.getAttribute(classOf[Dispatcher])
          if (dispatcherAttr.isPresent && dispatcherAttr.get().dispatcher.nonEmpty) {
            mater.system.dispatchers.lookup(dispatcherAttr.get().dispatcher)
          } else {
            mater.executionContext
          }
        }
        buffer = EvictingQueue.create(parallelism)
        retriesBuffer = EvictingQueue.create(parallelism)
        createClient(true)
      }

      private def createClient(withPull: Boolean): Unit = {
        val cb = getAsyncCallback[Try[C]] {
          case scala.util.Success(res) ⇒
            client = res
            if (isAvailable(in)) onPush()
            if (withPull) onPull()
          case scala.util.Failure(t) ⇒ teleporterFailure(t)
        }
        try {
          create(executionContext).onComplete(cb.invoke)
        } catch {
          case NonFatal(ex) ⇒ teleporterFailure(ex)
        }
      }

      def futureCompleted(result: (In, Try[Out])): Unit = {
        val (inElem, r) = result
        r match {
          case Success(elem) if elem != null ⇒
            retries = 0
            inFlight -= 1
            if (isAvailable(out)) {
              if (!hasBeenPulled(in)) tryPull(in)
              push(out, elem)
            } else buffer.add(elem)
          case other ⇒
            val ex = other match {
              case Failure(t) ⇒ retriesBuffer.add(inElem); t
              case Success(s) if s == null ⇒ new NullPointerException("result can't be null")
            }
            teleporterFailure(ex)
        }
      }

      private val futureCB = getAsyncCallback(futureCompleted)
      private val invokeFutureCB: ((In, Try[Out])) ⇒ Unit = futureCB.invoke

      override def onPush(): Unit = {
        try {
          if (client == null) return
          val elem = if (!retriesBuffer.isEmpty) {
            retriesBuffer.remove()
          } else {
            val grabElem = grab(in)
            inFlight += 1
            grabElem
          }
          val future = write(client, elem, executionContext)
          future.value match {
            case None ⇒ future.onComplete(invokeFutureCB(elem, _))
            case Some(v) ⇒ futureCompleted((elem, v))
          }
        } catch {
          case NonFatal(ex) ⇒ teleporterFailure(ex)
        }
        if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      override def onPull(): Unit = {
        if (!buffer.isEmpty) push(out, buffer.remove())
        else if (isClosed(in) && todo == 0) closeAndThen(completeStage)

        if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFinish(): Unit = {
        logger.info("Sink Is down stream really finish, onUpstreamFinish")
        if (todo == 0 && !isClosed(in)) closeAndThen(super.onUpstreamFinish)
      }

      setHandlers(in, out, this)


      @scala.throws[Exception](classOf[Exception])
      override def onDownstreamFinish(): Unit = {
        logger.info("Sink Is down stream really finish, onDownstreamFinish")
        closeAndThen(super.onDownstreamFinish)
      }

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFailure(ex: Throwable): Unit = {
        logger.info("Sink Is down stream really finish, onUpstreamFailure", ex)
        closeAndThen(() ⇒ super.onUpstreamFailure(ex))
      }

      protected def closeAndThen(f: () ⇒ Unit): Unit = {
        setKeepGoing(true)
        val cb = getAsyncCallback[Try[Done]] {
          case scala.util.Success(_) ⇒ f()
          case scala.util.Failure(t) ⇒ failStage(t)
        }
        try {
          if (client != null) {
            close(client, executionContext).onComplete(cb.invoke)
          }
        } catch {
          case NonFatal(ex) ⇒ failStage(ex)
        }
      }

      @scala.throws[Exception](classOf[Exception])
      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case (ex: Throwable, d: integration.supervision.Supervision.Directive) ⇒ decide(ex, d)
          case _ ⇒ log.warning(s"Unmatched timerKey: $timerKey")
        }
      }

      override def supervisionStrategy: SupervisionStrategy = inheritedAttributes.get[SupervisionStrategy].getOrElse(TeleporterAttributes.emptySupervisionStrategy)

      override def teleporterFailure(ex: Throwable): Unit = {
        super.teleporterFailure(ex)
        matchRule(ex) match {
          case Some(rule) ⇒
            if (rule.directive.retries == -1 || (retriesBuffer.isEmpty || (!retriesBuffer.isEmpty && retries / retriesBuffer.size() < rule.directive.retries))) {
              rule.directive.delay match {
                case Duration.Zero ⇒ decide(ex, rule.directive)
                case d: FiniteDuration ⇒ scheduleOnce((ex, rule.directive), d)
                case _ ⇒
              }
              retries += 1
            } else {
              rule.directive.next.foreach(decide(ex, _))
            }
          case None ⇒ stop(ex)
        }
      }

      override def reload(ex: Throwable): Unit = {
        close(client, executionContext)
        preStart()
      }

      override def retry(ex: Throwable): Unit = {
        if (!retriesBuffer.isEmpty) teleporterFailure(ex) //not good
      }

      override def resume(ex: Throwable): Unit = {
        inFlight -= 1
        if (isClosed(in) && todo == 0) closeAndThen(completeStage)
        else if (!hasBeenPulled(in)) tryPull(in)
      }

      override def stop(ex: Throwable): Unit = {
        close(client, executionContext)
        failStage(ex)
      }
    }
}