package teleporter.integration.component.file

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util
import java.util.Base64

import akka.stream.scaladsl.{Flow, FlowOpsMat, Framing, Keep, Sink, Source}
import akka.stream.{Attributes, Materializer, TeleporterAttributes, ThrottleMode}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component.SinkRoller.SinkRollerSetting
import teleporter.integration.component.{CommonSink, CommonSource, JdbcMessage, SinkRoller}
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics
import teleporter.integration.utils.{Command, MapBean}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by huanwuji 
  * date 2017/2/17.
  */
object FileStreams {
  def sourceAck(sourceKey: String)(implicit center: TeleporterCenter): Source[AckMessage[MapBean, ByteString], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val sourceConfig = sourceContext.config.mapTo[FileSourceMetaBean]
    source(sourceKey).map(m ⇒ SourceMessage(sourceConfig.offset(m.coordinate), m.data))
      .via(SourceAck.flow[ByteString](sourceContext.id, sourceContext.config))
  }

  val Read: util.Set[StandardOpenOption] = java.util.Collections.singleton(java.nio.file.StandardOpenOption.READ)

  def source(sourceKey: String)(implicit center: TeleporterCenter): Source[SourceMessage[Long, ByteString], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val sourceConfig = sourceContext.config.mapTo[FileSourceMetaBean]
    val source = Source.fromGraph(new FileSource(
      name = sourceKey,
      path = Paths.get(sourceConfig.path),
      offset = sourceConfig.offset,
      len = sourceConfig.len,
      bufferSize = sourceConfig.bufferSize))
      .addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sourceKey, sourceContext.config)))
    var offset = sourceConfig.offset
    val delimiterLength = sourceConfig.delimiter.map(_.length).getOrElse(0)
    sourceConfig.delimiter.map(bs ⇒ source.via(Framing.delimiter(bs, Int.MaxValue, allowTruncation = true)))
      .getOrElse(source).map { bs ⇒
      offset += (bs.length + delimiterLength)
      SourceMessage(offset, bs)
    }.via(Metrics.count[SourceMessage[Long, ByteString]](sourceKey)(center.metricsRegistry))
  }

  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[FileByteString], Message[FileByteString], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[FileSinkMetaBean]
    val fileFlow = Flow.fromGraph(new FileSink(
      name = sinkKey,
      path = sinkConfig.path,
      openOptions = sinkConfig.openOptions,
      offset = sinkConfig.offset
    )).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
      .via(Metrics.count[Message[FileByteString]](sinkKey)(center.metricsRegistry))
    SinkRollerSetting(sinkConfig) match {
      case Some(setting) ⇒
        if (setting.cron.isDefined || setting.size.isDefined) {
          val rollerFlow = SinkRoller.flow[Message[FileByteString], Message[FileByteString]](
            setting = setting,
            cronRef = center.crontab,
            catchSize = _.data.byteString.length,
            catchField = _.data.path.getOrElse(sinkConfig.path.get),
            rollKeep = (in, path) ⇒ in.map(data ⇒ data.copy(path = Some(path))),
            rollDo = path ⇒ Message(FileByteString(ByteString.empty, Some(path), total = 0))
          )
          rollerFlow.via(fileFlow)
        } else {
          fileFlow
        }
      case None ⇒ fileFlow
    }
  }

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[FileByteString], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def tail(path: Path)(implicit mater: Materializer): Source[ByteString, NotUsed] = {
    FileTailer.source(path)
  }

  def list(path: Path): Array[String] = {
    path.getParent.toFile.list()
  }

  def scan(path: Path, offset: Long, limit: Long): Source[ByteString, NotUsed] = {
    Source.fromGraph(new FileSource(
      path = path,
      offset = offset,
      len = limit,
      bufferSize = 8092))
  }
}

object FileCmd {

  trait Cmd

  case class Tail(offset: Long = 0, path: Path = Paths.get(".")) extends Cmd

  object Tail {
    val parser = new scopt.OptionParser[Tail]("tail") {
      opt[Int]("offset").optional().text("skip offset").action((x, c) ⇒ c.copy(offset = x))
      arg[String]("<path>").required().action((x, c) ⇒ c.copy(path = Paths.get(x)))
    }

    def parse(cmd: Seq[String]): Option[Source[ByteString, NotUsed]] = {
      parser.parse(cmd, Tail()).map(tail ⇒ FileTailer.source(path = tail.path, offset = tail.offset))
    }
  }

  case class Scan(offset: Long = 0, limit: Long = Int.MaxValue, path: Path = Paths.get(".")) extends Cmd

  object Scan {
    val parser = new scopt.OptionParser[Scan]("scan") {
      opt[Int]("offset").optional().text("skip text").action((x, c) ⇒ c.copy(offset = x))
      opt[Int]("limit").optional().text("scan limit").action((x, c) ⇒ c.copy(limit = x))
      arg[String]("<path>").required().action((x, c) ⇒ c.copy(path = Paths.get(x)))
    }

    def parse(cmd: Seq[String]): Option[Source[ByteString, NotUsed]] = {
      parser.parse(cmd, Scan()).map(scan ⇒ Source.fromGraph(new FileSource(
        path = scan.path,
        offset = scan.offset,
        len = scan.limit,
        bufferSize = 8092)))
    }
  }

  case class Ls(path: Path = Paths.get(".")) extends Cmd

  object Ls {
    val parser = new scopt.OptionParser[Ls]("ls") {
      arg[String]("<path>").required().action((x, c) ⇒ c.copy(path = Paths.get(x)))
    }

    def parse(cmd: Seq[String]): Option[Array[String]] = {
      parser.parse(cmd, Ls()).map(ls ⇒ ls.path.toFile.list())
    }
  }

  case class Grep(pattern: String = "")

  object Grep {
    val parser = new scopt.OptionParser[Grep]("grep") {
      arg[String]("<pattern>").required().text("grep pattern").action((x, c) ⇒ c.copy(pattern = x))
    }

    def parse(cmd: Seq[String]): Option[Flow[ByteString, ByteString, NotUsed]] = {
      parser.parse(cmd, Grep()).map(grep ⇒ Flow[ByteString].filter(_.utf8String.contains(grep.pattern)))
    }
  }

  case class Separator(s: String = File.separator)

  object Separator {
    val parser = new scopt.OptionParser[Separator]("st") {
      arg[String]("<split>").required().text("separator string").action((x, c) ⇒ c.copy(s = x))
    }

    def parse(cmd: Seq[String]): Option[Flow[ByteString, ByteString, NotUsed]] = {
      parser.parse(cmd, Separator()).map(separator ⇒ Framing.delimiter(ByteString(separator.s), maximumFrameLength = 1024 * 1024, allowTruncation = true))
    }
  }

  case class RateLimiter(rate: Int = 10)

  object RateLimiter {
    val parser = new scopt.OptionParser[RateLimiter]("rate") {
      arg[Int]("<rate>").required().text("output rate").action((x, c) ⇒ c.copy(rate = x))
    }

    def parse(cmd: Seq[String]): Option[Flow[ByteString, ByteString, NotUsed]] = {
      parser.parse(cmd, RateLimiter()).map(rateLimiter ⇒ Flow[ByteString].throttle(
        elements = rateLimiter.rate,
        per = 1.seconds,
        maximumBurst = 10,
        ThrottleMode.shaping
      ))
    }
  }

  def exec(cmd: String): Source[ByteString, NotUsed] = {
    val pipeStreams: Array[FlowOpsMat[ByteString, NotUsed]] = cmd.split("\\|").map(_.replaceFirst("^\\s+", ""))
      .map(Command.cmdSplit)
      .flatMap {
        case "tail" :: tail ⇒ Tail.parse(tail)
        case "scan" :: tail ⇒ Scan.parse(tail)
        case "grep" :: tail ⇒ Grep.parse(tail)
        case "ls" :: tail ⇒ Ls.parse(tail).map(files ⇒ Source(files.map(ByteString(_)).toIndexedSeq))
        case "st" :: tail ⇒ Separator.parse(tail)
        case "rate" :: tail ⇒ RateLimiter.parse(tail)
      }
    if (pipeStreams.isEmpty) Source.single(ByteString("Cmd not found"))
    else if (pipeStreams.length == 1) pipeStreams.head.asInstanceOf[Source[ByteString, NotUsed]]
    else pipeStreams.tail.foldLeft[Source[ByteString, NotUsed]](pipeStreams.head.asInstanceOf[Source[ByteString, NotUsed]]) { (s, s1) ⇒
      s1 match {
        case source: Source[ByteString, NotUsed] ⇒ s.concat(source)
        case flow: Flow[ByteString, ByteString, NotUsed] ⇒ s.via(flow)
      }
    }
  }
}

class FileSourceMetaBean(override val underlying: JdbcMessage) extends SourceMetaBean(underlying) {
  val FPath = "path"
  val FOffset = "offset"
  val FLen = "len"
  val FBufferSize = "bufferSize"
  val FDelimiter = "delimiter"

  def path: String = client[String](FPath)

  def offset: Long = client.get[Long](FOffset).getOrElse(-1)

  def offset(offset: Long): MapBean = MapBean(this ++ (FClient, FOffset → offset))

  def len: Long = client.get[Long](FLen).getOrElse(Long.MaxValue)

  def bufferSize: Int = client.get[Int](FBufferSize).getOrElse(4096)

  def delimiter: Option[ByteString] = client.get[String](FDelimiter).map(Base64.getDecoder.decode(_)).map(ByteString(_))
}

class FileSource(name: String = "FileSource", path: Path, offset: Long, len: Long = Long.MaxValue, bufferSize: Int)
  extends CommonSource[FileChannel, ByteString](name) {
  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.IODispatcher

  val buf: ByteBuffer = ByteBuffer.allocate(bufferSize)
  var bytesRemaining: Long = len - offset
  var bytesRead = 0

  override def create(): FileChannel = {
    val fileChannel = FileChannel.open(path, FileStreams.Read)
    if (offset == -1) fileChannel else fileChannel.position(offset)
  }

  override def readData(client: FileChannel): Option[ByteString] = {
    if (bytesRemaining <= 0) {
      None
    } else {
      bytesRead = client.read(buf)
      if (bytesRead >= 0) {
        bytesRemaining -= bytesRead
        buf.flip()
        val bs = ByteString.fromByteBuffer(buf)
        buf.clear()
        Some(bs)
      } else None
    }
  }

  override def close(client: FileChannel): Unit = client.close()
}

class FileSinkMetaBean(override val underlying: JdbcMessage) extends SinkMetaBean(underlying) {
  val FPath = "path"
  val FOffset = "offset"
  val FOpenOptions = "openOptions"

  def path: Option[String] = client.get[String](FPath)

  def offset: Long = client.get[Long](FOffset).getOrElse(-1)

  def openOptions: Set[StandardOpenOption] = {
    client[String](FOpenOptions).replaceAll("\\s+", "").split(",").toSet.map(StandardOpenOption.valueOf)
  }
}

case class FileByteString(byteString: ByteString, path: Option[String] = None, end: Long = 0, total: Long = -1) {
  def start: Long = end - byteString.length

  def isEnd: Boolean = end == total
}

case class WriteStatus(fileChannel: FileChannel, var lastPosition: Long)

class FileSink(name: String = "FileSink", path: Option[String], openOptions: Set[StandardOpenOption], offset: Long = -1)
  extends CommonSink[NotUsed, Message[FileByteString], Message[FileByteString]](name, identity) with Logging {
  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.IODispatcher

  val fileChannels: mutable.HashMap[String, WriteStatus] = mutable.HashMap[String, WriteStatus]()

  override def create(): NotUsed = NotUsed

  private def createPath(path: Path): Unit = {
    val parent = path.getParent
    if (!Files.exists(parent)) {
      Files.createDirectories(path.getParent)
    }
    logger.info(s"Create $path")
  }

  override def write(client: NotUsed, elem: Message[FileByteString]): Message[FileByteString] = {
    val fileByteString = elem.data
    val currPath = fileByteString.path.getOrElse(path.get)
    val writeStatus = fileChannels.getOrElseUpdate(currPath, {
      val filePath = Paths.get(currPath)
      createPath(filePath)
      val fileChannel = FileChannel.open(filePath, openOptions.asJava)
      val position = if (fileByteString.start > 0) {
        fileChannel.position(fileByteString.start)
        fileByteString.start
      } else if (offset != -1) {
        fileChannel.position(offset)
        offset
      } else {
        -1
      }
      WriteStatus(fileChannel, position)
    })
    if (fileByteString.start > 0) {
      if (writeStatus.lastPosition != fileByteString.start) {
        writeStatus.fileChannel.position(fileByteString.start)
      }
      writeStatus.fileChannel.write(fileByteString.byteString.asByteBuffer)
      writeStatus.lastPosition = fileByteString.end
    } else {
      writeStatus.fileChannel.write(fileByteString.byteString.asByteBuffer)
    }
    if (fileByteString.isEnd) closeStream(currPath)
    elem
  }

  private def closeStream(path: String): Unit = {
    try {
      logger.info(s"Closed $path")
      fileChannels.remove(path).foreach(_.fileChannel.close())
    } catch {
      case e: Throwable ⇒ logger.info(e.getMessage, e)
    }
  }

  override def close(client: NotUsed): Unit = {
    fileChannels.keys.foreach(closeStream)
    fileChannels.clear()
  }
}