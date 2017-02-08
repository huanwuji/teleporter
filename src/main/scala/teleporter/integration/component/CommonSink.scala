package teleporter.integration.component

import akka.Done
import akka.stream.TeleporterAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.stage._
import com.google.common.collect.EvictingQueue
import teleporter.integration
import teleporter.integration.supervision.SinkDecider

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Created by huanwuji 
  * date 2016/12/29.
  */
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
      override def onDownstreamFinish(): Unit = closeStage()

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFailure(ex: Throwable): Unit = {
        try {
          close(client)
          failStage(ex)
        } catch {
          case NonFatal(ex1) ⇒ failStage(ex1)
        }
      }

      override def onUpstreamFinish(): Unit = closeStage()

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
          case _ ⇒ log.warning(s"Unmatched timeKey: $timerKey")
        }
      }

      override def supervisionStrategy: SupervisionStrategy = inheritedAttributes.get[SupervisionStrategy].getOrElse(TeleporterAttributes.emptySupervisionStrategy)

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
        createClient()
        retry(ex)
      }

      override def retry(ex: Throwable): Unit = lastElem match {
        case Some(elem) ⇒ writeData(elem)
        case None ⇒
      }

      override def resume(ex: Throwable): Unit = if (!isClosed(in)) pull(in)

      override def stop(ex: Throwable): Unit = {
        close(client)
        failStage(ex)
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
          val dispatcherAttr = inheritedAttributes.getAttribute(classOf[TeleporterAttributes.Dispatcher])
          if (dispatcherAttr.isPresent) {
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
        else if (isClosed(in) && todo == 0) completeStage()

        if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (todo == 0) completeStage()
      }

      setHandlers(in, out, this)


      @scala.throws[Exception](classOf[Exception])
      override def onDownstreamFinish(): Unit = closeStage()

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFailure(ex: Throwable): Unit = {
        try {
          close(client, executionContext)
          failStage(ex)
        } catch {
          case NonFatal(ex1) ⇒ failStage(ex1)
        }
      }

      private def closeStage(): Unit =
        try {
          close(client, executionContext)
          completeStage()
        } catch {
          case NonFatal(ex) ⇒ failStage(ex)
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
        matchRule(ex).foreach { rule ⇒
          if (retries / retriesBuffer.size() < rule.directive.retries || rule.directive.retries == 0) {
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
        close(client, executionContext)
        preStart()
      }

      override def retry(ex: Throwable): Unit = {
        if (!retriesBuffer.isEmpty) teleporterFailure(ex) //not good
      }

      override def resume(ex: Throwable): Unit = {
        inFlight -= 1
        if (isClosed(in) && todo == 0) completeStage()
        else if (!hasBeenPulled(in)) tryPull(in)
      }

      override def stop(ex: Throwable): Unit = {
        close(client, executionContext)
        failStage(ex)
      }
    }
}