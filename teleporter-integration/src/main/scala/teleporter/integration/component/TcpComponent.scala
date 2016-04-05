package teleporter.integration.component

import java.nio.ByteOrder

import akka.stream.io.Framing
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

/**
 * date 2015/8/3.
 * @author daikui
 */
object TcpComponent extends LazyLogging {
  def lengthFraming(maximumFrameLength: Int) = Framing.lengthField(4, 0, maximumFrameLength + 4, ByteOrder.BIG_ENDIAN).map(_.drop(4))

  def addLengthHeader(message: ByteString): ByteString = {
    val header = ByteString(
      (message.size >> 24) & 0xFF,
      (message.size >> 16) & 0xFF,
      (message.size >> 8) & 0xFF,
      message.size & 0xFF)
    header ++ message
  }
}