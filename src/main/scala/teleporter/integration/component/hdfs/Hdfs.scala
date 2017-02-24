package teleporter.integration.component.hdfs

import java.io.{InputStream, OutputStream}
import java.util.{Base64, Properties}

import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.util.ByteString
import akka.{Done, NotUsed}
import io.leopard.javahost.JavaHost
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import teleporter.integration.component.{CommonSink, CommonSource, JdbcMessage}
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics
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
    val source = Source.fromGraph(new HdfsSource(
      path = new Path(sourceConfig.path),
      offset = sourceConfig.offset,
      len = sourceConfig.len,
      bufferSize = sourceConfig.bufferSize,
      _create = () ⇒ center.context.register(addressKey, bind, () ⇒ address(addressKey)).client,
      _close = {
        _ ⇒
          center.context.unRegister(addressKey, bind)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sourceKey, sourceContext.config)))
    var offset = sourceConfig.offset
    val delimiterLength = sourceConfig.delimiter.map(_.length).getOrElse(0)
    sourceConfig.delimiter.map(bs ⇒ source.via(Framing.delimiter(bs, Int.MaxValue, allowTruncation = true)))
      .getOrElse(source).map { bs ⇒
      offset += (bs.length + delimiterLength)
      SourceMessage(offset, bs)
    }
      .via(Metrics.count[SourceMessage[Long, ByteString]](sourceKey)(center.metricsRegistry))
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
        _ ⇒ center.context.unRegister(addressKey, bind)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
      .via(Metrics.count[Message[ByteString]](sinkKey)(center.metricsRegistry))
  }

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[ByteString], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def address(addressKey: String)(implicit center: TeleporterCenter): AutoCloseClientRef[FileSystem] = {
    val config = center.context.getContext[AddressContext](addressKey).config.mapTo[HdfsMetaBean]
    config.hosts.foreach { hosts ⇒
      val properties = new Properties()
      properties.load(IOUtils.toInputStream(hosts))
      JavaHost.updateVirtualDns(properties)
    }
    val conf = new Configuration(false)
    config.coreSite.foreach(t ⇒ conf.addResource(IOUtils.toInputStream(t)))
    config.hdfsSite.foreach(t ⇒ conf.addResource(IOUtils.toInputStream(t)))
    config.sslClient.foreach(t ⇒ conf.addResource(IOUtils.toInputStream(t)))
    val fileSystem = FileSystem.get(conf)
    AutoCloseClientRef[FileSystem](addressKey, fileSystem)
  }
}

object HdfsSourceMetaBean {
  val FPath = "path"
  val FOffset = "offset"
  val FLen = "len"
  val FBufferSize = "bufferSize"
  val FDelimiter = "delimiter"
}

class HdfsSourceMetaBean(override val underlying: JdbcMessage) extends SourceMetaBean(underlying) {

  import HdfsSourceMetaBean._
  import SourceMetaBean._

  def path: String = client[String](FPath)

  def offset: Long = client.get[Long](FOffset).getOrElse(0)

  def offset(offset: Long): MapBean = MapBean(this ++ (FClient, FOffset → offset))

  def len: Long = client.get[Long](FLen).getOrElse(Long.MaxValue)

  def bufferSize: Int = client.get[Int](FBufferSize).getOrElse(4096)

  def delimiter: Option[ByteString] = client.get[String](FDelimiter).map(Base64.getDecoder.decode(_)).map(ByteString(_))
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
  extends CommonSource[FileSystem, ByteString]("HdfsReader") {
  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.CacheDispatcher

  val buf = new Array[Byte](bufferSize)
  var bytesRemaining: Long = len - offset
  var bytesRead = 0
  var inputStream: InputStream = _

  override def create(): FileSystem = {
    val fs = _create()
    inputStream = fs.open(path)
    inputStream.skip(offset)
    fs
  }

  override def readData(client: FileSystem): Option[ByteString] = {
    val bytesToRead = if (bytesRemaining < buf.length) bytesRemaining else buf.length
    bytesRead = inputStream.read(buf, 0, bytesToRead.toInt)
    if (bytesRead >= 0) {
      bytesRemaining -= bytesRead
      Some(ByteString.fromArray(buf, 0, bytesRead))
    } else None
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
  val FHosts = "hosts"
  val FUri = "uri"
  val FUser = "user"
  val FCoreSite = "core-site.xml"
  val FHdfsSite = "hdfs-site.xml"
  val FSSLClient = "ssl-client.xml"
}

class HdfsMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import HdfsMetaBean._

  def hosts: Option[String] = client.get[String](FHosts)

  def uri: String = client[String](FUri)

  def user: Option[String] = client.get[String](FUser)

  def coreSite: Option[String] = client.get[String](FCoreSite)

  def hdfsSite: Option[String] = client.get[String](FHdfsSite)

  def sslClient: Option[String] = client.get[String](FSSLClient)
}