package teleporter.integration.component.file

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path}

import akka.actor.{Cancellable, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.file.FileTailerPublisher.Deliver

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * @author kui.dai Created 2016/9/6
  */
object FileTailerComponent

object FileTailerPublisher {
  val Read = java.util.Collections.singleton(java.nio.file.StandardOpenOption.READ)

  case object Deliver

  def props(path: Path, end: Boolean = true): Props = Props(classOf[FileTailerPublisher], path, end)
}

class FileTailerPublisher(path: Path, end: Boolean) extends ActorPublisher[ByteString] with LazyLogging {

  import context.dispatcher

  private val inbuf: ByteBuffer = ByteBuffer.allocate(4096)
  var schedule: Cancellable = _
  private var chan: FileChannel = _
  private var position: Long = _
  var last: Long = 0

  override def preStart() = {
    init()
    super.preStart()
  }

  def init(): Unit = {
    try {
      inbuf.clear()
      chan = FileChannel.open(path, FileTailerPublisher.Read)
      last = System.currentTimeMillis
      position = if (end) chan.size else 0
      chan.position(position)
      schedule = context.system.scheduler.schedule(1.second, 1.second, self, Deliver)
    } catch {
      case NonFatal(ex) ⇒
        logger.error(ex.getLocalizedMessage, ex)
        onErrorThenStop(ex)
    }
  }

  override def receive: Receive = {
    case Request(n) ⇒ delivery()
    case Cancel ⇒ context.stop(self)
    case Deliver ⇒ delivery()
  }

  @tailrec
  final def delivery(): Unit = {
    if (totalDemand > 0 && isActive) {
      val newer: Boolean = Files.getLastModifiedTime(path).toMillis > last // IO-279, must be done first
      // Check the file length to see if it was rotated
      val length: Long = chan.size()
      if (length < position) {
        logger.info("File was rotated")
        if (chan != null) chan.close()
        init()
      } else {
        // File was not rotated
        // See if the file needs to be read again
        if (length > position) {
          // The file has more content than it did last time
          onNext(read())
          last = System.currentTimeMillis
          delivery()
        } else if (newer) {
          /*
           * This can happen if the file is truncated or overwritten with the exact same length of
           * information. In cases like this, the file position needs to be reset
          */
          position = 0
          chan.position(position) // cannot be null here
          onNext(read())
          last = System.currentTimeMillis
          delivery()
        }
      }
    }
  }

  def read(): ByteString =
    (try chan.read(inbuf) catch {
      case NonFatal(ex) ⇒
        logger.error(ex.getLocalizedMessage, ex)
        onErrorThenStop(ex)
        Int.MinValue
    }) match {
      case -1 | 0 | Int.MinValue ⇒ ByteString.empty
      case readBytes ⇒
        inbuf.flip()
        val bs = ByteString.fromByteBuffer(inbuf)
        inbuf.clear()
        position = chan.position()
        bs
    }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    if (chan != null) {
      schedule.cancel()
      chan.close()
    }
    super.postStop()
  }
}
