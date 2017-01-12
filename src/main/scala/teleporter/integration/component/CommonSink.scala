package teleporter.integration.component

import akka.Done
import akka.stream.TeleporterAttribute.SupervisionStrategy
import akka.stream._
import akka.stream.stage._
import com.google.common.collect.EvictingQueue
import teleporter.integration
import teleporter.integration.supervision.SinkDecider

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
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
          if (!isClosed(in)) pull(in)
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
          case _ ⇒ log.warning(s"Unmatched $timerKey")
        }
      }

      override def supervisionStrategy: SupervisionStrategy = inheritedAttributes.get[SupervisionStrategy].getOrElse(TeleporterAttribute.emptySupervisionStrategy)

      override def teleporterFailure(ex: Throwable): Unit = {
        retries += 1
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

      override def reload(ex: Throwable): Unit = createClient()

      override def retry(ex: Throwable): Unit = writeData(lastElem.get)

      override def resume(ex: Throwable): Unit = if (!isClosed(in)) pull(in)

      override def stop(ex: Throwable): Unit = failStage(ex)

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
      implicit val executionContext: ExecutionContextExecutor = {
        val mater = materializer.asInstanceOf[ActorMaterializer]
        val dispatcherAttr = inheritedAttributes.getAttribute(classOf[TeleporterAttribute.Dispatcher])
        if (dispatcherAttr.isPresent) {
          mater.system.dispatchers.lookup(dispatcherAttr.get().dispatcher)
        } else {
          mater.executionContext
        }
      }
      private var inFlight = 0
      private var retriesBuffer: EvictingQueue[In] = _
      private var buffer: EvictingQueue[Out] = _
      var resource: Promise[C] = Promise[C]()
      var client: C = _
      var retries: Int = _
      var retrying: Boolean = false

      private[this] def todo = inFlight + buffer.size()

      override def preStart(): Unit = {
        buffer = EvictingQueue.create(parallelism)
        retriesBuffer = EvictingQueue.create(parallelism)
        createClient(false)
      }

      private def createClient(withPull: Boolean): Unit = {
        val cb = getAsyncCallback[Try[C]] {
          case scala.util.Success(res) ⇒
            resource.success(res)
            if (withPull) onPull()
          case scala.util.Failure(t) ⇒ teleporterFailure(t)
        }
        try {
          create(executionContext).onComplete(cb.invoke)
        } catch {
          case NonFatal(ex) ⇒ teleporterFailure(ex)
        }
      }

      def futureCompleted(result: Try[Out]): Unit = {
        retries = 0
        result match {
          case Success(elem) if elem != null ⇒
            inFlight -= 1
            if (isAvailable(out)) {
              if (!hasBeenPulled(in)) tryPull(in)
              push(out, elem)
            } else buffer.add(elem)
          case other ⇒
            val ex = other match {
              case Failure(t) ⇒ t
              case Success(s) if s == null ⇒ throw new NullPointerException("result can't be null")
            }
            teleporterFailure(ex)
        }
      }

      private val futureCB = getAsyncCallback(futureCompleted)
      private val invokeFutureCB: Try[Out] ⇒ Unit = futureCB.invoke

      override def onPush(): Unit = {
        try {
          val elem = if (retrying && !retriesBuffer.isEmpty) {
            val retryElem = retriesBuffer.remove()
            retrying = false
            retryElem
          } else grab(in)
          val future = write(client, elem, executionContext)
          inFlight += 1
          future.value match {
            case None ⇒ future.onComplete(invokeFutureCB)
            case Some(v) ⇒ futureCompleted(v)
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
          case _ ⇒ log.warning(s"Unmatched $timerKey")
        }
      }

      override def supervisionStrategy: SupervisionStrategy = inheritedAttributes.get[SupervisionStrategy].getOrElse(TeleporterAttribute.emptySupervisionStrategy)

      override def teleporterFailure(ex: Throwable): Unit = {
        retries += 1
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

      override def reload(ex: Throwable): Unit = preStart()

      override def retry(ex: Throwable): Unit = {
        retrying = true
        onPush()
      }

      override def resume(ex: Throwable): Unit = {
        inFlight -= 1
        if (isClosed(in) && todo == 0) completeStage()
        else if (!hasBeenPulled(in)) tryPull(in)
      }

      override def stop(ex: Throwable): Unit = failStage(ex)
    }
}