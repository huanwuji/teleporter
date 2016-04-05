package teleporter.integration.component.file

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.actor._
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.util.ByteString
import teleporter.integration.SourceControl.ErrorThenStop
import teleporter.integration.component.{FileRecord, TeleporterFileRecord}
import teleporter.integration.conf.Conf.Source
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core.{Component, TId, TeleporterCenter}
import teleporter.integration.transaction.{BatchCommitTransaction, RecoveryPoint, TransactionConf}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Created by yadong.li on 2016/1/19.
 */
case class FileMessage(data: ByteString, filePath: String, var append: Boolean = true, position: Long)

class FilePublisher(override val id: Int)(implicit teleporterCenter: TeleporterCenter)
  extends ActorPublisher[TeleporterFileRecord] with PropsSupport
  with Component with BatchCommitTransaction[FileRecord, Source]
  with ActorLogging {

  import FilePublisher._

  implicit val recoveryPoint: RecoveryPoint[Source] = teleporterCenter.defaultRecoveryPoint

  var conf: Conf.Source = teleporterCenter.sourceFactory.loadConf(id)
  var propsMap: Map[String, Any] = conf.props
  override val transactionConf: TransactionConf = TransactionConf(conf.props)
  cmpMap = propsMap.get("cmp").get.asInstanceOf[scala.collection.immutable.Map[String, Any]]

  var cmpMap: scala.collection.immutable.Map[String, Any] = _
  var offsetMap: mutable.HashMap[String, Any] = mutable.HashMap[String,Any]()

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

      chunkSize = getIntOrElse(cmpMap, "chunkSize", 8192)
      maxBuffer = getIntOrElse(cmpMap, "maxBuffer", 100)
      batchSize = transactionConf.batchSize

      require(chunkSize > 0, s"chunkSize must be > 0 (was $chunkSize)")
      byteStringSendNum = getLongOrElse(cmpMap, "byteStringSendNum", 0)
      offset = byteStringSendNum * chunkSize
      val filename = getStringOrElse(cmpMap, "file", "")
      logger.info(s"fileName:filename")
      f = new File(filename)
      buf = ByteBuffer.allocate(chunkSize)
      raf = new RandomAccessFile(f, "r")
      fileChan = raf.getChannel
      fileChan = fileChan.position(offset)
      offsetMap ++= cmpMap
    } catch {
      case ex: Exception ⇒
        log.error(ex.getLocalizedMessage, ex)
        onErrorThenStop(ex)
    }

  }

  override def postRestart(reason: Throwable) = {

    log.error(reason.getLocalizedMessage, reason)

  }

  override def postStop(): Unit = {

    // cmpClose()
    super.postStop()
    fileChan.close()
    raf.close()
    log.info(s"${f.getName} Publisher will stop")

  }

  def receive: Receive = {

    case Request(n) ⇒ readAndSignal(maxBuffer)
    case Continue ⇒ readAndSignal(maxBuffer)
    case tid: TId ⇒ end(tid)
    case ErrorThenStop(reason: Throwable) ⇒
      // cmpClose()
      onErrorThenStop(reason)
    case ActorPublisherMessage.Cancel ⇒
      //   cmpClose();
      context.stop(self)
  }

  def readAndSignal(maxReadAhead: Int): Unit =
    if (isActive) {
      // Write previously buffered, read into buffer, write newly buffered
      availableChunks = signalOnNexts(readAhead(maxReadAhead, signalOnNexts(availableChunks)))
      if (totalDemand > 0 && isActive) self ! Continue
    }

  //  private def cmpClose() = {
  //    if (!isClosed) {
  //      teleporterCenter.removeAddress(conf)
  //      isClosed = true
  //    }
  //  }

  @tailrec private def signalOnNexts(chunks: Vector[ByteString]): Vector[ByteString] = {

    //   val recoverIt = recovery
    //    recoverIt.nonEmpty &&
    //
    //  while (recoverIt.hasNext && totalDemand > 0) {
    //    val message = recoverIt.next()
    //    message.data.isReSend = true
    //    logger.info(s"$message")
    //    onNext(recovery.next())
    //  }

    if (chunks.nonEmpty && totalDemand > 0) {
      //    val content: ByteString = ByteString()
      //    var size:Int = 0
      //    if(chunks.size>batchSize){
      //      size = batchSize
      //    }else {
      //      size =chunks.size
      //    }
      val content: ByteString = chunks.head
      //      byteStringSendNum += size
      val position: Long = byteStringSendNum * chunkSize
      byteStringSendNum += 1
      val fileMessage = FileMessage(data = content, filePath = f.getName, position = position)
      offsetMap += ("byteStringSendNum" → byteStringSendNum)
      propsMap += ("cmp" → offsetMap.toMap)
      conf = conf.copy(props = propsMap)
      begin(conf, fileMessage) {
        msg ⇒ onNext(msg)
      }
      signalOnNexts(chunks.tail)
    } else {
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

  def props(id: Int) = {
    Props(classOf[FilePublisher], id).withDeploy(Deploy.local)
  }

  private case object Continue extends DeadLetterSuppression

}