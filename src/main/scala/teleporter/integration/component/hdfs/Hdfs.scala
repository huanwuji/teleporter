package teleporter.integration.component.hdfs

import java.io.{InputStream, OutputStream}
import java.security.PrivilegedExceptionAction
import java.util.{Base64, Properties}

import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.util.ByteString
import akka.{Done, NotUsed}
import io.leopard.javahost.JavaHost
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component.SinkRoller.SinkRollerSetting
import teleporter.integration.component.{CommonSink, CommonSource, JdbcMessage, SinkRoller}
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics
import teleporter.integration.utils.MapBean

import scala.collection.mutable
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
      name = sourceKey,
      path = sourceConfig.path,
      offset = sourceConfig.offset,
      len = sourceConfig.len,
      bufferSize = sourceConfig.bufferSize,
      _create = () ⇒ center.context.register(addressKey, bind, () ⇒ addressByUser(addressKey, sourceConfig.user)).client,
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
    }.via(Metrics.count[SourceMessage[Long, ByteString]](sourceKey)(center.metricsRegistry))
  }

  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[HdfsByteString], Message[HdfsByteString], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[HdfsSinkMetaBean]
    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    val hdfsFlow = Flow.fromGraph(new HdfsSink(
      name = sinkKey,
      path = sinkConfig.path,
      overwrite = sinkConfig.overwrite,
      _create = () ⇒ {
        center.context.register(addressKey, bind, () ⇒ addressByUser(addressKey, sinkConfig.user)).client
      },
      _close = {
        _ ⇒ center.context.unRegister(addressKey, bind)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
      .via(Metrics.count[Message[HdfsByteString]](sinkKey)(center.metricsRegistry))
    SinkRollerSetting(sinkConfig) match {
      case Some(setting) ⇒
        if (setting.cron.isDefined || setting.size.isDefined) {
          val rollerFlow = SinkRoller.flow[Message[HdfsByteString], Message[HdfsByteString]](
            setting = setting,
            cronRef = center.crontab,
            catchSize = _.data.byteString.length,
            catchField = _.data.path.getOrElse(sinkConfig.path.get),
            rollKeep = (in, path) ⇒ in.map(data ⇒ data.copy(path = Some(path))),
            rollDo = path ⇒ Message(HdfsByteString(ByteString.empty, Some(path), total = 0))
          )
          rollerFlow.via(hdfsFlow)
        } else {
          hdfsFlow
        }
      case None ⇒ hdfsFlow
    }
  }

  def addressByUser(addressKey: String, user: Option[String])(implicit center: TeleporterCenter): AutoCloseClientRef[FileSystem] = {
    user match {
      case Some(u) ⇒ UserGroupInformation.createRemoteUser(u).doAs(new PrivilegedExceptionAction[AutoCloseClientRef[FileSystem]] {
        override def run(): AutoCloseClientRef[FileSystem] = address(addressKey)
      })
      case None ⇒ address(addressKey)
    }
  }

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[HdfsByteString], Future[Done]] = {
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
  val FUser = "user"
}

class HdfsSourceMetaBean(override val underlying: JdbcMessage) extends SourceMetaBean(underlying) {

  import HdfsSourceMetaBean._

  def path: String = client[String](FPath)

  def offset: Long = client.get[Long](FOffset).getOrElse(0)

  def offset(offset: Long): MapBean = MapBean(this ++ (FClient, FOffset → offset))

  def len: Long = client.get[Long](FLen).getOrElse(Long.MaxValue)

  def bufferSize: Int = client.get[Int](FBufferSize).getOrElse(4096)

  def delimiter: Option[ByteString] = client.get[String](FDelimiter).map(Base64.getDecoder.decode(_)).map(ByteString(_))

  def user: Option[String] = client.get[String](FUser)
}

object HdfsSinkMetaBean {
  val FPath = "path"
  val FUser = "user"
  val FOverwrite = "overwrite"
}

class HdfsSinkMetaBean(override val underlying: JdbcMessage) extends SinkMetaBean(underlying) {

  import HdfsSinkMetaBean._

  def path: Option[String] = client.get[String](FPath)

  def user: Option[String] = client.get[String](FUser)

  def overwrite: Boolean = client.get[Boolean](FOverwrite).getOrElse(true)
}

class HdfsSource(name: String = "HdfsReader",
                 path: String,
                 offset: Long, len: Long = Long.MaxValue, bufferSize: Int,
                 _create: () ⇒ FileSystem,
                 _close: FileSystem ⇒ Unit)
  extends CommonSource[FileSystem, ByteString](name) {
  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.CacheDispatcher

  val buf = new Array[Byte](bufferSize)
  var bytesRemaining: Long = len - offset
  var bytesRead = 0
  var inputStream: InputStream = _

  override def create(): FileSystem = {
    val fs = _create()
    inputStream = fs.open(new Path(path))
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

case class HdfsByteString(byteString: ByteString, path: Option[String] = None, end: Long = 0, total: Long = -1) {
  def isEnd: Boolean = end == total
}

class HdfsSink(name: String = "HdfsWriter",
               path: Option[String],
               overwrite: Boolean,
               _create: () ⇒ FileSystem,
               _close: FileSystem ⇒ Unit)
  extends CommonSink[FileSystem, Message[HdfsByteString], Message[HdfsByteString]](name, identity) with Logging {
  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.CacheDispatcher
  val outputStreams: mutable.HashMap[String, OutputStream] = mutable.HashMap[String, OutputStream]()

  override def create(): FileSystem = _create()

  override def write(client: FileSystem, elem: Message[HdfsByteString]): Message[HdfsByteString] = {
    val hdfsByteString = elem.data
    val currPath = hdfsByteString.path.getOrElse(path.get)
    outputStreams.getOrElseUpdate(currPath, client.create(new Path(currPath), overwrite))
      .write(hdfsByteString.byteString.toArray)
    if (hdfsByteString.isEnd) closeStream(currPath)
    elem
  }

  private def closeStream(path: String): Unit = {
    try {
      outputStreams.remove(path).foreach(_.close())
    } catch {
      case e: Throwable ⇒ logger.info(e.getMessage, e)
    }
  }

  override def close(client: FileSystem): Unit = {
    outputStreams.keys.foreach(closeStream)
    outputStreams.clear()
    _close(client)
  }
}

class HdfsMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {
  val FHosts = "hosts"
  val FUri = "uri"
  val FCoreSite = "core-site.xml"
  val FHdfsSite = "hdfs-site.xml"
  val FSSLClient = "ssl-client.xml"

  def hosts: Option[String] = client.get[String](FHosts)

  def uri: String = client[String](FUri)

  def coreSite: Option[String] = client.get[String](FCoreSite)

  def hdfsSite: Option[String] = client.get[String](FHdfsSite)

  def sslClient: Option[String] = client.get[String](FSSLClient)
}