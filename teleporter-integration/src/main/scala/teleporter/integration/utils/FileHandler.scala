package teleporter.integration.utils

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.coding.Gzip
import akka.stream.ActorMaterializer
import akka.stream.TLSProtocol.{SendBytes, SessionBytes, SslTlsInbound, SslTlsOutbound}
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component._
import teleporter.integration.core.{TId, TeleporterCenter}
import teleporter.integration.proto.FileBuf.{FileProto, FileProtos}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

/**
 * Created by yadong.li on 2016/1/20.
 */

case class FileTransportHandler(center: TeleporterCenter, outgoingConn: Flow[SslTlsOutbound, SslTlsInbound, NotUsed])(implicit system: ActorSystem, materlizer: ActorMaterializer) extends LazyLogging {

  @tailrec
  final def transportFileCheck(source: String): Unit = {

    val result = transportFile(source)
    if (result) {
      logger.info("File Task completes")
    } else {
      logger.info("Session Truncated and reStart the Stream")
      transportFileCheck(source)
    }
  }

  def transportFile(source: String): Boolean = {

    val promise = Promise[Boolean]()
    var send = 0
    var receive = 0
    var result = false
    center.source[TeleporterFileRecord](source).map {
      message: TeleporterFileRecord ⇒
        send += 1
        val data = message.data
        val fileProto = FileProto.newBuilder().setAppend(data.append).setFilePath(data.filePath).setPosition(data.position)
          .setTId(com.google.protobuf.ByteString.copyFrom(message.id.toBytes))
          .setMessage(com.google.protobuf.ByteString.copyFrom(message.data.data.toByteBuffer))
        val fileProtos = FileProtos.newBuilder()
        fileProtos.addProtos(fileProto).build().toByteArray
    }.map(ByteString(_)).map(Gzip.encode)
      .map(TcpComponent.addLengthHeader)
      .map(SendBytes).via(outgoingConn)
      .map {
        case SessionBytes(a, b) ⇒ b
        case x ⇒
          promise.trySuccess(false)
          throw new RuntimeException("Session was closed")
      }.via(TcpComponent.lengthFraming(10 * 1024 * 1024))
      .to(Sink.foreach {
        x: ByteString ⇒
          x.toArray.grouped(TId.length).foreach {
            bytes ⇒
              val tId = TId.keyFromBytes(bytes)
              center.actor(tId.persistenceId) ! tId
              receive += 1
              if (send <= receive) {
                promise.trySuccess(true)
              }
          }
      }).run()
    result = Await.result(promise.future, 1.days)
    result

  }


}