package teleporter.integration.core

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.pattern._
import teleporter.integration.CmpType
import teleporter.integration.SourceControl.{CompleteThenStop, Notify}
import teleporter.integration.StreamControl._
import teleporter.integration.conf.Conf.StreamStatus
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core.StreamManager._
import teleporter.integration.script.{GitTemplateLoader, ScriptEngines}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Author: kui.dai
 * Date: 2016/1/11.
 */
trait StreamConfig

trait SingleSourceStreamConfig extends StreamConfig {
  val sourceId: Int
}

case class StreamContext(maxParallelismSource: Int = Int.MaxValue, //Limit resources size when concurrent call it, like: connectionPool
                         currParallelismSource: AtomicInteger = new AtomicInteger())

case class StreamRef(taskId: Int = -1, streamId: Int = -1, sourceIds: Set[Int] = Set(), sinkIds: Set[Int] = Set(), addressIds: Set[Int] = Set()) {
  def addSourceId(sourceId: Int) = this.copy(sourceIds = this.sourceIds + sourceId)

  def addSink(sinkId: Int) = this.copy(sinkIds = this.sinkIds + sinkId)

  def addAddress(addressId: Int) = this.copy(addressIds = this.addressIds + addressId)

  def removeSourceId(sourceId: Int) = this.copy(sourceIds = this.sourceIds - sourceId)

  def removeSink(sinkId: Int) = this.copy(sinkIds = this.sinkIds - sinkId)

  def removeAddress(addressId: Int) = this.copy(addressIds = this.addressIds - addressId)
}

trait StreamRefHolder {
  val streamRefs = TrieMap[Int, StreamRef]()

  def register(conf: Any, streamId: Int = -1): Unit =
    conf match {
      case source: Conf.Source ⇒ streamRefs += source.streamId → getStreamRef(source.streamId).addSourceId(source.id)
      case sink: Conf.Sink ⇒ streamRefs += sink.streamId → getStreamRef(sink.streamId).addSourceId(sink.id)
      case address: Conf.Address ⇒ streamRefs += streamId → getStreamRef(streamId).addSourceId(address.id)
      case _ ⇒ throw new UnsupportedOperationException(s"Register unsupport type $conf")
    }

  def getStreamRef(streamId: Int) = streamRefs.getOrElseUpdate(streamId, StreamRef())
}

object StreamManager {

  trait StreamStatus

  object Starting extends StreamStatus

  object Restarting extends StreamStatus

  object Running extends StreamStatus

  object Closing extends StreamStatus

  object Closed extends StreamStatus

  case class StreamState(status: StreamStatus = Running, restartTime: Long = 0L)

  type StreamInvoke = (Conf.Stream, TeleporterCenter) ⇒ Any

  def apply(taskId: Int)(implicit center: TeleporterCenter): ActorRef = center.system.actorOf(Props(classOf[DefaultStreamManagerImpl], taskId, center))
}

trait StreamManager[T] extends Actor with ActorLogging {
  val taskId: Int
  val center: TeleporterCenter

  import context.dispatcher

  val streamStates = mutable.Map[T, StreamState]()
  val cancellable = context.system.scheduler.schedule(10.seconds, 10.seconds, self, Scan)

  override def receive: Actor.Receive = {
    case Start(streamConfig: T) ⇒ _streamStart(streamConfig)
    case Stop(streamConfig: T) ⇒ stop(streamConfig)
    case Scan ⇒
      streamStates.filter {
        entry ⇒
          val streamState = entry._2
          System.currentTimeMillis() > streamState.restartTime && streamState.status == Closed
      }.map {
        x ⇒
          log.info(s"Will restart $x")
          safeStart(x._1)
          x._1
      }
    case Restart(streamConfig: T, delay) ⇒ restart(streamConfig, delay)
    case TryStart(streamConfig: T) ⇒ tryStart(streamConfig)
  }

  def tryStart(streamConfig: T): Unit =
    streamStates.get(streamConfig) match {
      case Some(streamState) if streamState.status != Running ⇒ _streamStart(streamConfig)
      case _ ⇒ //ignore this
    }

  def restart(streamConfig: T, delay: Duration): Unit = {
    delay match {
      case Duration.Zero ⇒ safeStart(streamConfig)
      case d: FiniteDuration ⇒ switchState(streamConfig, Closed, delay.toMillis + System.currentTimeMillis())
      case _ ⇒
    }
  }

  def safeStart(streamConfig: T): Unit = {
    switchState(streamConfig, Starting)
    stop(streamConfig, _streamStart)
  }

  def stop(streamConfig: T, onSuccess: T ⇒ Unit = streamConfig ⇒ {}): Unit

  private def _streamStart(streamConfig: T): Unit = {
    switchState(streamConfig, Starting)
    streamStart(streamConfig)
    switchState(streamConfig, Running)
  }

  def switchState(streamConfig: T, status: StreamStatus, restartTime: Long = 0L) =
    streamStates += (streamConfig → StreamState(status, restartTime))

  def streamStart(streamConfig: T): Unit

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    cancellable.cancel()
  }
}

object EmptyStreamManager extends StreamManager[Conf.Stream] {
  override val taskId: Int = -1

  override def stop(streamConfig: Conf.Stream, onSuccess: (Conf.Stream) ⇒ Unit): Unit = ???

  override def streamStart(streamConfig: Conf.Stream): Unit = ???

  override val center: TeleporterCenter = null
}

trait SingleSourceStreamManager[T <: SingleSourceStreamConfig] extends StreamManager[T] {

  import context.dispatcher

  def stop(streamConfig: T, onSuccess: T ⇒ Unit = streamConfig ⇒ {}): Unit = {
    switchState(streamConfig, Closing)
    val sourceId = streamConfig.sourceId
    val sourceRef = if (center.actorAddresses.exists(sourceId, CmpType.Shadow)) {
      center.shadowActor(sourceId)
    } else {
      center.actor(sourceId, CmpType.Source)
    }
    gracefulStop(sourceRef, 2.minutes, CompleteThenStop).onComplete {
      case Success(v) ⇒ switchState(streamConfig, Closed); onSuccess(streamConfig)
      case Failure(ex) ⇒ log.error(ex.getLocalizedMessage, ex)
    }
  }
}

trait DefaultStreamManager extends StreamManager[Conf.Stream] with StreamRefHolder with GitTemplateLoader with PropsSupport {
  val taskId: Int
  private val streamInvokeCache = mutable.WeakHashMap[Conf.Stream, StreamInvoke]()

  import center.{materializer, system}
  import context.dispatcher

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    center.configFactory.streams(taskId).filterNot(_.status == StreamStatus.INVALID).foreach(streamStart)
  }

  override def receive: Actor.Receive = {
    case StreamCmpRegister(conf, streamId) ⇒ register(conf, streamId)
    case NotifyStream(streamId) ⇒ getStreamRef(streamId).sourceIds.foreach(center.actor(_, CmpType.Source) ! Notify)
    case x ⇒ super.receive(x)
  }

  override def streamStart(stream: Conf.Stream): Unit = {
    loadStreamInvoke(stream)(stream, center)
  }

  def stop(stream: Conf.Stream, onSuccess: Conf.Stream ⇒ Unit = streamConfig ⇒ {}): Unit = {
    switchState(stream, Closing)
    val sourceId = getIntOpt(stream.props, "source").get
    val sourceRef = if (center.actorAddresses.exists(sourceId, CmpType.Shadow)) {
      center.shadowActor(sourceId)
    } else {
      center.actor(sourceId, CmpType.Source)
    }
    gracefulStop(sourceRef, 2.minutes, CompleteThenStop).onComplete {
      case Success(v) ⇒ switchState(stream, Closed); onSuccess(stream)
      case Failure(ex) ⇒ log.error(ex.getLocalizedMessage, ex)
    }
  }

  private def loadStreamInvoke(stream: Conf.Stream): StreamInvoke = {
    streamInvokeCache.getOrElseUpdate(stream, {
      log.info(s"Load stream $stream")
      val maxParallelismSource = getIntOrElse(stream.props, "maxParallelismSource", Int.MaxValue)
      center.streamContext(stream.id, StreamContext(maxParallelismSource))
      val task = center.task(stream.taskId)
      val streamTemplateOpt = getStringOpt(stream.props, "streamDef")
      val taskStreamTemplateOpt = getStringOpt(task.props, "streamDef")
      val template = if (streamTemplateOpt.isDefined && streamTemplateOpt.get.nonEmpty) {
        streamTemplateOpt.get
      } else {
        taskStreamTemplateOpt.get
      }
      val script = template match {
        case s if s.startsWith("git") ⇒ loadTemplate(s)
        case s ⇒ s
      }
      ScriptEngines.scala.eval(script).asInstanceOf[StreamInvoke]
    })
  }
}

class DefaultStreamManagerImpl(val taskId: Int)(implicit val center: TeleporterCenter) extends DefaultStreamManager