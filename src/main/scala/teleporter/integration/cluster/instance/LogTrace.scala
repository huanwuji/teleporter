package teleporter.integration.cluster.instance

import java.nio.file.{Path, Paths}

import akka.actor.{Actor, Cancellable}
import akka.stream.scaladsl.{Framing, Sink, SinkQueueWithCancel}
import akka.util.ByteString
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.RollingFileAppender
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.instance.LogTrace.{Check, Line, LogTailer}
import teleporter.integration.cluster.rpc.EventBody.{LogTailRequest, LogTailResponse}
import teleporter.integration.cluster.rpc.TeleporterEvent
import teleporter.integration.cluster.rpc.fbs.{EventType, Role}
import teleporter.integration.component.file.FileTailer
import teleporter.integration.core.TeleporterCenter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._

/**
  * @author kui.dai Created 2016/9/1
  */
class LogTrace()(implicit center: TeleporterCenter) extends Actor with Logging {

  import center.materializer
  import context.dispatcher

  var logTailer: LogTailer = _
  var checkSchedule: Cancellable = _
  var currTime: Long = System.currentTimeMillis()
  val eventQueue: mutable.Queue[TeleporterEvent[LogTailRequest]] = mutable.Queue[TeleporterEvent[LogTailRequest]]()
  val logs: mutable.Queue[Line] = mutable.Queue[Line]()

  override def receive: Receive = {
    case event: TeleporterEvent[LogTailRequest] ⇒
      currTime = System.currentTimeMillis()
      if (logTailer == null) {
        createLog4j2Tailer()
      }
      eventQueue += event
      delivery()
    case line@Line(s) ⇒
      if (eventQueue.isEmpty) {
        logs += line
      } else {
        center.brokers ! SendMessage(TeleporterEvent(eventType = EventType.LogTail, role = Role.Response, body = LogTailResponse(s)))
        if (eventQueue.nonEmpty) delivery()
      }
    case Check ⇒
      if (System.currentTimeMillis() - currTime > 4.minutes.toMillis) destroy() else delivery()
  }

  def delivery(): Unit = {
    if (logs.nonEmpty) {
      self ! logs.dequeue()
    } else {
      logTailer.queue.pull().foreach {
        case Some(bs) ⇒ self ! Line(bs.utf8String)
        case None ⇒
      }
    }
  }

  def createLog4j2Tailer(): Unit = {
    val loggerContext = LogManager.getContext(true).asInstanceOf[LoggerContext]
    val path = loggerContext.getConfiguration.getAppenders.asScala.collect {
      case (_, v: RollingFileAppender) ⇒ Paths.get(v.getFileName)
    }.head
    val queue = FileTailer.source(path)
      .via(Framing.delimiter(ByteString.fromString("\n"), 1024 * 5))
      .runWith(Sink.queue())
    this.logTailer = LogTailer(path, queue)
    checkSchedule = context.system.scheduler.schedule(2.seconds, 2.seconds, self, Check)
  }

  def destroy(): Unit = {
    if (logTailer != null) {
      logTailer.queue.cancel()
      checkSchedule.cancel()
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    this.destroy()
  }
}

object LogTrace {

  case object Check

  case object Delivery

  case class Line(s: String)

  case class LogTailer(path: Path, queue: SinkQueueWithCancel[ByteString])

}