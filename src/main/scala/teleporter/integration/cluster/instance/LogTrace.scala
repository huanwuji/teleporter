package teleporter.integration.cluster.instance

import java.nio.file.Paths

import akka.actor.{Actor, Cancellable}
import akka.stream.scaladsl.{Framing, Sink, SinkQueueWithCancel, Source}
import akka.util.ByteString
import com.google.common.io.Resources
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.instance.LogTrace.{Check, Line, LogTailer}
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.broker.Broker.LogRequest
import teleporter.integration.cluster.rpc.proto.instance.Instance.LogResponse
import teleporter.integration.component.file.FileTailerPublisher
import teleporter.integration.core.TeleporterCenter

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * @author kui.dai Created 2016/9/1
  */
class LogTrace()(implicit center: TeleporterCenter) extends Actor with LazyLogging {

  import center.materializer
  import context.dispatcher

  var logTailer: LogTailer = _
  var checkSchedule: Cancellable = _
  var currTime: Long = System.currentTimeMillis()
  val eventQueue = mutable.Queue[TeleporterEvent]()
  val logs = mutable.Queue[Line]()

  override def receive: Receive = {
    case event: TeleporterEvent ⇒
      currTime = System.currentTimeMillis()
      event.getType match {
        case EventType.LogRequest ⇒
          val logRequest = LogRequest.parseFrom(event.getBody)
          if (logTailer == null) {
            createLogTailer(logRequest.getCmd)
          } else if (logTailer.path != logRequest.getCmd) {
            destroy()
            createLogTailer(logRequest.getCmd)
          }
          eventQueue += event
      }
      delivery()
    case line@Line(s) ⇒
      if (eventQueue.isEmpty) {
        logs += line
      } else {
        center.brokers ! SendMessage(TeleporterEvent.newBuilder(eventQueue.dequeue())
          .setType(EventType.LogResponse)
          .setBody(LogResponse.newBuilder().setLine(s).build().toByteString)
          .build())
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

  def createLogTailer(path: String): Unit = {
    val _path = Paths.get(Resources.getResource(getClass, "/").toURI).getParent.getParent.getParent.getParent.resolve("logs/server.log")
    val queue = Source.actorPublisher[ByteString](FileTailerPublisher.props(_path))
      .via(Framing.delimiter(ByteString.fromString("\n"), 1024 * 5))
      .runWith(Sink.queue())
    this.logTailer = LogTailer(path, queue)
    checkSchedule = context.system.scheduler.schedule(2.seconds, 2.seconds, self, Check)
  }

  def destroy(): Unit = {
    if (logTailer != null) {
      logTailer.queue.cancel()
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    this.destroy()
    checkSchedule.cancel()
  }
}

object LogTrace {

  case object Check

  case object Delivery

  case class Line(s: String)

  case class LogTailer(path: String, queue: SinkQueueWithCancel[ByteString])

}