package teleporter.integration.component.file

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import java.util

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic
import akka.stream.{Attributes, TeleporterAttributes}
import akka.util.ByteString
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component.{CommonSource, CommonSourceGraphStageLogic}

import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * @author kui.dai Created 2016/9/6
  */
object FileTailer {
  val Read: util.Set[StandardOpenOption] = java.util.Collections.singleton(java.nio.file.StandardOpenOption.READ)

  def source(path: Path, offset: Long = 0, end: Boolean = true): Source[ByteString, NotUsed] = {
    Source.fromGraph(new FileTailer(path, offset, end))
  }
}

class FileTailer(path: Path, offset: Long, end: Boolean, bufferSize: Int = 4096) extends CommonSource[FileChannel, ByteString]("file.tailer") with Logging {

  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.IODispatcher

  private val inbuf: ByteBuffer = ByteBuffer.allocate(bufferSize)
  var schedule: Cancellable = _
  private var chan: FileChannel = _
  private var position: Long = _
  var last: Long = 0

  override def create(): FileChannel = {
    inbuf.clear()
    chan = FileChannel.open(path, FileTailer.Read)
    last = System.currentTimeMillis
    position = if (offset > 0) chan.size - offset else {
      if (end) chan.size else 0
    }
    chan.position(position)
    chan
  }

  override def readData(client: FileChannel): Option[ByteString] = {
    // IO-279, must be done first
    // Check the file length to see if it was rotated
    val length: Long = chan.size()
    if (length < position) {
      logger.info("File was rotated")
      if (chan != null) chan.close()
      create()
      readData(client)
    } else {
      // File was not rotated
      // See if the file needs to be read again
      if (length > position) {
        // The file has more content than it did last time
        last = System.currentTimeMillis
        read()
      } else {
        None
      }
    }
  }

  private def read(): Option[ByteString] =
    (try chan.read(inbuf) catch {
      case NonFatal(ex) ⇒
        logger.error(ex.getLocalizedMessage, ex)
        Int.MinValue
    }) match {
      case -1 | 0 | Int.MinValue ⇒ None
      case _ ⇒
        inbuf.flip()
        val bs = ByteString.fromByteBuffer(inbuf)
        inbuf.clear()
        position = chan.position()
        Some(bs)
    }

  override def close(client: FileChannel): Unit = {
    chan.close()
  }

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new CommonSourceGraphStageLogic(shape, create, readData, close, inheritedAttributes) {
    override protected def pushData(): Unit = {
      readData(client) match {
        case None ⇒ scheduleOnce('pull, 1.seconds)
        case Some(elem) ⇒ push(shape.out, elem)
      }
    }

    @scala.throws[Exception](classOf[Exception]) override protected
    def onTimer(timerKey: Any): Unit = {
      timerKey match {
        case 'pull ⇒ pushData()
        case _ ⇒ super.onTimer(timerKey)
      }
    }
  }
}