package teleporter.integration.compress

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.GZIPInputStream

import akka.http.scaladsl.coding._
import akka.http.scaladsl.model.HttpMessage
import org.apache.commons.io.IOUtils

/**
 * date 2015/8/3.
 * @author daikui
 */
class TeleporterGzip(override val messageFilter: HttpMessage ⇒ Boolean, maxBytes: Int) extends Gzip(messageFilter) with Coder with StreamDecoder {
  override def maxBytesPerChunk: Int = maxBytes
}

object TeleporterGzip {
  def apply(maxBytes: Int) = new TeleporterGzip(Encoder.DefaultFilter, maxBytes)

  def decode(is: InputStream, os: OutputStream): Unit = {
    IOUtils.copy(new GZIPInputStream(is), os)
  }

  def decode(bytes: Array[Byte]): Array[Byte] = {
    val is = new ByteArrayInputStream(bytes)
    val os = new ByteArrayOutputStream(bytes.length * 3)
    decode(is, os)
    os.toByteArray
  }
}