package akka.stream.impl.io

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import akka.actor.{ActorLogging, Deploy, Props}
import akka.stream.ActorAttributes.Dispatcher
import akka.stream.actor.{ActorSubscriberMessage, WatermarkRequestStrategy}
import akka.stream.impl.SinkModule
import akka.stream.impl.Stages.DefaultAttributes._
import akka.stream.impl.StreamLayout.Module
import akka.stream.{ActorMaterializer, Attributes, MaterializationContext, SinkShape}
import akka.util.ByteString

import scala.concurrent.{Future, Promise}

/**
 * Author: kui.dai
 * Date: 2015/12/1.
 */
case class DestFile(file: File, append: Boolean)

case class FileOutput(destFile: DestFile) extends AutoCloseable {
  val raf = new RandomAccessFile(destFile.file, "rw")
  val chan = raf.getChannel
  if (destFile.append) chan.position(chan.size())

  def close(): Unit = {
    if (chan ne null) chan.close()
    if (raf ne null) raf.close()
  }
}

private[akka] final class TeleporterFileSink(f: File, append: Boolean, val attributes: Attributes, shape: SinkShape[ByteString], actorNameOpt: Option[String] = None)
  extends SinkModule[ByteString, Future[Long]](shape) {

  override def create(context: MaterializationContext) = {
    val mat = ActorMaterializer.downcast(context.materializer)
    val settings = mat.effectiveSettings(context.effectiveAttributes)

    val bytesWrittenPromise = Promise[Long]()
    val props = TeleporterFileSubscriber.props(f, bytesWrittenPromise, settings.maxInputBufferSize, append)
    val dispatcher = context.effectiveAttributes.get[Dispatcher](IODispatcher).dispatcher

    val ref = actorNameOpt match {
      case Some(actorName) ⇒ mat.system.actorOf(props.withDispatcher(dispatcher), actorName)
      case None ⇒ mat.actorOf(context, props.withDispatcher(dispatcher))
    }
    (akka.stream.actor.ActorSubscriber[ByteString](ref), bytesWrittenPromise.future)
  }

  override protected def newInstance(shape: SinkShape[ByteString]): SinkModule[ByteString, Future[Long]] =
    new FileSink(f, append, attributes, shape)

  override def withAttributes(attr: Attributes): Module =
    new FileSink(f, append, attr, amendShape(attr))
}

private[akka] object TeleporterFileSubscriber {
  def props(f: File, completionPromise: Promise[Long], bufSize: Int, append: Boolean) = {
    require(bufSize > 0, "buffer size must be > 0")
    Props(classOf[FileSubscriber], f, completionPromise, bufSize, append).withDeploy(Deploy.local)
  }

}

/** INTERNAL API */
private[akka] class TeleporterFileSubscriber(f: File, bytesWrittenPromise: Promise[Long], bufSize: Int, append: Boolean)
  extends akka.stream.actor.ActorSubscriber
  with ActorLogging {

  override protected val requestStrategy = WatermarkRequestStrategy(highWatermark = bufSize)

  private var currFile: File = _
  private var raf: RandomAccessFile = _
  private var chan: FileChannel = _

  private var bytesWritten: Long = 0

  override def preStart(): Unit = try {
    initResource(f, append)
    super.preStart()
  } catch {
    case ex: Exception ⇒
      bytesWrittenPromise.failure(ex)
      cancel()
  }

  def receive = {
    case ActorSubscriberMessage.OnNext(bytes: ByteString) ⇒
      try {
        bytesWritten += chan.write(bytes.asByteBuffer)
      } catch {
        case ex: Exception ⇒
          bytesWrittenPromise.failure(ex)
          cancel()
      }

    case ActorSubscriberMessage.OnError(cause) ⇒
      log.error(cause, "Tearing down FileSink({}) due to upstream error", f.getAbsolutePath)
      context.stop(self)

    case ActorSubscriberMessage.OnComplete ⇒
      try {
        chan.force(true)
      } catch {
        case ex: Exception ⇒
          bytesWrittenPromise.failure(ex)
      }
      context.stop(self)
    case DestFile(file, _append) ⇒
      closeResource()
      if (currFile.getPath != file.getPath) {
        try {
          initResource(file, _append)
        } catch {
          case ex: Exception ⇒
            bytesWrittenPromise.failure(ex)
            cancel()
        }
      }
  }

  private def initResource(file: File, append: Boolean): Unit = {
    currFile = file
    raf = new RandomAccessFile(file, "rw")
    chan = raf.getChannel
    if (append) chan.position(chan.size())
  }

  private def closeResource(): Unit = {
    if (chan ne null) chan.close()
    if (raf ne null) raf.close()
  }

  override def postStop(): Unit = {
    bytesWrittenPromise.trySuccess(bytesWritten)
    closeResource()
    super.postStop()
  }
}

object FileComponent