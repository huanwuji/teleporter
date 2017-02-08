package teleporter.integration.component.hdfs

import java.io.{InputStream, OutputStream}
import java.net.URI

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import teleporter.integration.component.{CommonSink, CommonSource, JdbcMessage}
import teleporter.integration.core._
import teleporter.integration.utils.MapBean

import scala.concurrent.Future

/**
  * Created by joker on 15/10/9
  */
object Hdfs {
  def sourceAck(sourceKey: String)(implicit center: TeleporterCenter): Source[AckMessage[MapBean, ByteString], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val sourceConfig = sourceContext.config.mapTo[HdfsSourceMetaBean]
    source(sourceKey).map(m ⇒ SourceMessage(sourceConfig.offset(m.coordinate), m.data))
      .via(SourceAck.flow[ByteString](sourceContext.id, sourceContext.config))
  }

  def source(sourceKey: String)(implicit center: TeleporterCenter): Source[SourceMessage[Long, ByteString], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val sourceConfig = sourceContext.config.mapTo[HdfsSourceMetaBean]
    val bind = Option(sourceConfig.addressBind).getOrElse(sourceKey)
    val addressKey = sourceContext.address().key
    Source.fromGraph(new HdfsSource(
      path = new Path(sourceConfig.path),
      offset = sourceConfig.offset,
      len = sourceConfig.len,
      bufferSize = sourceConfig.bufferSize,
      _create = () ⇒ center.context.register(addressKey, bind, () ⇒ address(addressKey)).client,
      _close = {
        _ ⇒
          center.context.unRegister(sourceKey, bind)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sourceKey, sourceContext.config)))
  }

  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[ByteString], Message[ByteString], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[HdfsSinkMetaBean]
    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new HdfsSink(
      path = new Path(sinkConfig.path),
      overwrite = sinkConfig.overwrite,
      _create = () ⇒ {
        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
      },
      _close = {
        _ ⇒ center.context.unRegister(sinkKey, bind)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
  }

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[ByteString], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def address(addressKey: String)(implicit center: TeleporterCenter): AutoCloseClientRef[FileSystem] = {
    val config = center.context.getContext[AddressContext](addressKey).config.mapTo[HdfsMetaBean]
    val conf = new Configuration(false)
    val fileSystem = if (config.user.isEmpty) {
      FileSystem.get(new URI(config.uri), conf)
    } else {
      FileSystem.get(new URI(config.uri), conf, config.user)
    }
    AutoCloseClientRef[FileSystem](addressKey, fileSystem)
  }
}

object HdfsSourceMetaBean {
  val FPath = "path"
  val FOffset = "offset"
  val FLen = "len"
  val FBufferSize = "bufferSize"
}

class HdfsSourceMetaBean(override val underlying: JdbcMessage) extends SourceMetaBean(underlying) {

  import HdfsSourceMetaBean._

  def path: String = client[String](FPath)

  def offset: Long = client.get[Long](FOffset).getOrElse(0)

  def offset(offset: Long): MapBean = MapBean(this ++ (FOffset → offset))

  def len: Long = client.get[Long](FLen).getOrElse(Long.MaxValue)

  def bufferSize: Int = client.get[Int](FBufferSize).getOrElse(4096)
}

object HdfsSinkMetaBean {
  val FPath = "path"
  val FOverwrite = "overwrite"
}

class HdfsSinkMetaBean(override val underlying: JdbcMessage) extends SinkMetaBean(underlying) {

  import HdfsSinkMetaBean._

  def path: String = client[String](FPath)

  def overwrite: Boolean = client.get[Boolean](FOverwrite).getOrElse(true)
}

class HdfsSource(path: Path,
                 offset: Long, len: Long = Long.MaxValue, bufferSize: Int,
                 _create: () ⇒ FileSystem,
                 _close: FileSystem ⇒ Unit)
  extends CommonSource[FileSystem, SourceMessage[Long, ByteString]]("HdfsReader") {
  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.CacheDispatcher

  val buf = new Array[Byte](bufferSize)
  var bytesRemaining: Long = len - offset
  var bytesRead = 0
  var inputStream: InputStream = _

  override def create(): FileSystem = {
    val fs = _create()
    inputStream = fs.open(path)
    fs
  }

  override def readData(client: FileSystem): Option[SourceMessage[Long, ByteString]] = {
    val bytesToRead = if (bytesRemaining < buf.length) bytesRemaining else buf.length
    bytesRead = inputStream.read(buf, 0, bytesToRead.toInt)
    if (bytesRead == -1) {
      None
    } else {
      bytesRemaining -= bytesRead
      Some(SourceMessage(len - bytesRemaining, ByteString.fromArray(buf, 0, bytesToRead.toInt)))
    }
  }

  override def close(client: FileSystem): Unit = {
    if (inputStream != null) inputStream.close()
    _close(client)
  }
}

class HdfsSink(path: Path, overwrite: Boolean, _create: () ⇒ FileSystem, _close: FileSystem ⇒ Unit)
  extends CommonSink[FileSystem, Message[ByteString], Message[ByteString]]("HdfsWriter") {
  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.CacheDispatcher
  var outputStream: OutputStream = _

  override def create(): FileSystem = {
    val fs = _create()
    outputStream = fs.create(path, overwrite)
    fs
  }

  override def write(client: FileSystem, elem: Message[ByteString]): Message[ByteString] = {
    outputStream.write(elem.data.toArray)
    elem
  }

  override def close(client: FileSystem): Unit = {
    if (outputStream != null) outputStream.close()
    _close(client)
  }
}

object HdfsMetaBean {
  val FUri = "uri"
  val FConf = "conf"
  val FUser = "user"
}

class HdfsMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import HdfsMetaBean._

  def uri: String = client[String](FUri)

  def conf: String = client[String](FConf)

  def user: String = client[String](FUser)
}