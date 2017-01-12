package teleporter.integration.component.hdfs

import java.io.{FileNotFoundException, InputStream, OutputStream}
import java.net.URI

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Attributes, TeleporterAttribute}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import teleporter.integration.ClientApply
import teleporter.integration.component.{CommonSink, CommonSource}
import teleporter.integration.core._

import scala.concurrent.Future

/**
  * Created by joker on 15/10/9
  */
object Hdfs {
  def fromHDFS(fs: FileSystem, path: Path, delete: Boolean = false, offset: Long = 0, len: Long = Long.MaxValue, bufferSize: Int = 4096): Source[ByteString, NotUsed] = {
    Source.fromGraph(new HdfsSource(fs, path, delete, offset, len, bufferSize, _.close()))
  }

  def flow(fs: FileSystem, path: Path, overwrite: Boolean = false): Flow[ByteString, Unit, NotUsed] = {
    Flow.fromGraph(new HdfsSink(fs, path, overwrite, _.close()))
  }

  def toHDFS(fs: FileSystem, path: Path, overwrite: Boolean = false): Sink[ByteString, Future[Done]] = {
    flow(fs, path, overwrite).toMat(Sink.ignore)(Keep.right)
  }

  def address(addressKey: String, refKey: String)(implicit center: TeleporterCenter): AutoCloseClientRef[FileSystem] = {
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

class HdfsSource(fs: FileSystem, path: Path, delete: Boolean = false, offset: Long, len: Long, bufferSize: Int, _close: InputStream ⇒ Unit)
  extends CommonSource[InputStream, ByteString]("HdfsReader") {

  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttribute.CacheDispatcher

  val buf = new Array[Byte](bufferSize)
  val count = 0
  var bytesRemaining: Int = count
  var bytesRead = 0

  override def create(): InputStream = {
    if (!fs.exists(path)) {
      throw new FileNotFoundException(s"Source file $path not found")
    } else {
      val inputStream = fs.open(path, bufferSize)
      inputStream.skip(offset)
      inputStream
    }
  }

  override def readData(client: InputStream): Option[ByteString] = {
    val bytesToRead = if (bytesRemaining < buf.length) bytesRemaining else buf.length
    bytesRead = client.read(buf, 0, bytesToRead)
    if (bytesRead == -1) {
      None
    } else {
      bytesRemaining -= bytesRead
      Some(ByteString.fromArray(buf, 0, bytesToRead))
    }
  }

  override def close(client: InputStream): Unit = _close(client)
}

class HdfsSink(fs: FileSystem, path: Path, overwrite: Boolean, _close: OutputStream ⇒ Unit) extends CommonSink[OutputStream, ByteString, Unit]("HdfsWriter") {

  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttribute.CacheDispatcher

  override def create(): OutputStream = {
    fs.create(path, overwrite)
  }

  override def write(client: OutputStream, elem: ByteString): Unit = {
    client.write(elem.toArray)
  }

  override def close(client: OutputStream): Unit = _close(client)
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

object HdfsComponent {
  val hdfsApply: ClientApply = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config.mapTo[HdfsMetaBean]
    val conf = new Configuration(false)
    val fileSystem = if (config.user.isEmpty) {
      FileSystem.get(new URI(config.uri), conf)
    } else {
      FileSystem.get(new URI(config.uri), conf, config.user)
    }
    AutoCloseClientRef[FileSystem](key, fileSystem)
  }
}