package teleporter.integration.component.file

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.actor._
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.file.FilePublisher.Continue
import teleporter.integration.component.{FileRecord, TeleporterFileRecord}
import teleporter.integration.core.TeleporterConfig.SourceConfig
import teleporter.integration.core.TeleporterContext.Update
import teleporter.integration.core._
import teleporter.integration.transaction.Transaction
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{MapBean, MapMetadata}
import teleporter.integration.core.TeleporterConfig._

import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
  * Created by yadong.li on 2016/1/19.
  */
trait FilePublisherMetadata extends MapMetadata {
  val FClient = "client"
  val FChunkSize = "chunkSize"
  val FMaxBuffer = "maxBuffer"
  val FByteStringSendNum = "byteStringSendNum"
  val FFilename = "filename"
}

case class FileMessage(data: ByteString, filePath: String, var append: Boolean = true, position: Long)

class FilePublisher(override val key: String)(implicit val center: TeleporterCenter)
  extends ActorPublisher[TeleporterFileRecord]
    with Component
    with FilePublisherMetadata
    with LazyLogging {

  val sourceContext: SourceContext = center.context.getContext[SourceContext](key)

  val transaction = Transaction[FileRecord, SourceConfig](key, center.defaultRecoveryPoint)
  private var chunkSize: Int = _
  private var maxBuffer: Int = _
  private var raf: RandomAccessFile = _
  private var fileChan: FileChannel = _
  private var f: File = _
  private var batchSize: Int = _
  var offset: Long = _
  var byteStringSendNum: Long = _
  var isClosed = false
  var eofReachedAtOffset = Long.MinValue
  var buf: ByteBuffer = _
  var availableChunks: Vector[ByteString] = Vector.empty // TODO possibly resign read-ahead-ing and make fusable as Stage


  override def preStart() = {

    try {
      val filePublisherConfig = sourceContext.config[MapBean](FClient)
      chunkSize = filePublisherConfig.__dict__[Int](FChunkSize).getOrElse(8192)
      maxBuffer = filePublisherConfig.__dict__[Int](FMaxBuffer).getOrElse(100)

      require(chunkSize > 0, s"chunkSize must be > 0 (was $chunkSize)")
      byteStringSendNum = filePublisherConfig.__dict__[Long](FByteStringSendNum).getOrElse(0)
      offset = byteStringSendNum * chunkSize
      val filename = filePublisherConfig[String](FFilename)
      logger.info(s"fileName:filename")
      f = new File(filename)
      buf = ByteBuffer.allocate(chunkSize)
      raf = new RandomAccessFile(f, "r")
      fileChan = raf.getChannel
      fileChan = fileChan.position(offset)
    } catch {
      case ex: Exception ⇒
        logger.error(ex.getLocalizedMessage, ex)
        onErrorThenStop(ex)
    }
  }

  override def postRestart(reason: Throwable) = {

    logger.error(reason.getLocalizedMessage, reason)

  }

  override def postStop(): Unit = {

    // cmpClose()
    super.postStop()
    fileChan.close()
    raf.close()
    logger.info(s"${f.getName} Publisher will stop")
  }

  def receive: Receive = {
    case Request(n) ⇒ readAndSignal(maxBuffer)
    case Continue ⇒ readAndSignal(maxBuffer)
    case tid: TId ⇒ transaction.end(tid)
    case ActorPublisherMessage.Cancel ⇒ context.stop(self)
  }

  def readAndSignal(maxReadAhead: Int): Unit =
    if (isActive) {
      // Write previously buffered, read into buffer, write newly buffered
      availableChunks = signalOnNexts(readAhead(maxReadAhead, signalOnNexts(availableChunks)))
      if (totalDemand > 0 && isActive) self ! Continue
    }

  @tailrec private def signalOnNexts(chunks: Vector[ByteString]): Vector[ByteString] = {
    transaction.tryBegin(sourceContext.config, {
      if (chunks.nonEmpty && totalDemand > 0) {
        val content: ByteString = chunks.head
        val position: Long = byteStringSendNum * chunkSize
        byteStringSendNum += 1
        val fileMessage = FileMessage(data = content, filePath = f.getName, position = position)
        center.context.ref ! Update(sourceContext.copy(config = sourceContext.config ++ (FClient, FByteStringSendNum → byteStringSendNum)))
        Some(fileMessage)
      } else {
        None
      }
    }, onNext) match {
      case Transaction.Normal ⇒ signalOnNexts(chunks.tail)
      case Transaction.NoData ⇒
        if (chunks.isEmpty && eofEncountered)
          onCompleteThenStop()
        chunks
    }
  }

  @tailrec
  final def readAhead(maxChunks: Int, chunks: Vector[ByteString]): Vector[ByteString] =
    if (chunks.size <= maxChunks && isActive) {
      (try fileChan.read(buf) catch {
        case NonFatal(ex) ⇒ onErrorThenStop(ex); Int.MinValue
      }) match {
        case -1 ⇒ // EOF
          eofReachedAtOffset = fileChan.position
          logger.debug("No more bytes available to read (got `-1` from `read`), marking final bytes of file @ " + eofReachedAtOffset)
          chunks
        case 0 ⇒ readAhead(maxChunks, chunks) // had nothing to read into this chunk
        case Int.MinValue ⇒ Vector.empty // read failed, we're done here
        case readBytes ⇒
          //logger.info("one ByteString to buffer")
          buf.flip()
          val newChunks = chunks :+ ByteString.fromByteBuffer(buf)
          buf.clear()
          readAhead(maxChunks, newChunks)
      }
    } else {
      chunks
    }

  private final def eofEncountered: Boolean = eofReachedAtOffset != Long.MinValue


}

private object FilePublisher {

  def props(id: Long) = {
    Props(classOf[FilePublisher], id).withDeploy(Deploy.local)
  }

  private case object Continue extends DeadLetterSuppression

}