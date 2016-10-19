package teleporter.integration.utils

import javax.net.ssl.SSLContext

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.TLSProtocol.{NegotiateNewSession, SslTlsInbound, SslTlsOutbound}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component._
import teleporter.integration.component.tcp.TlsHelper
import teleporter.integration.core.{SourceContext, TId, TeleporterCenter}
import teleporter.integration.protocol.proto.FileBuf.{FileProto, FileProtos}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by yadong.li on 2016/1/20.
 */

case class FileTransportHandler(center: TeleporterCenter, tcpTransfer: Flow[SslTlsOutbound, SslTlsInbound, NotUsed])
                               (implicit system: ActorSystem, materlizer: ActorMaterializer, sslContext: SSLContext, newSession: NegotiateNewSession) extends LazyLogging {

  import system.dispatcher

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
    try {
      val future = center.source[TeleporterFileRecord](source)
        .map {
          message: TeleporterFileRecord ⇒
            val data = message.data
            val fileProto = FileProto.newBuilder().setAppend(data.append).setFilePath(data.filePath).setPosition(data.position)
              .setTId(com.google.protobuf.ByteString.copyFrom(message.id.toBytes))
              .setMessage(com.google.protobuf.ByteString.copyFrom(message.data.data.toByteBuffer))
            val fileProtos = FileProtos.newBuilder()
            fileProtos.addProtos(fileProto).build().toByteArray
        }.map(ByteString(_))
        .merge(Source.tick(5.seconds, 5.seconds, ByteString()))
        .via(TlsHelper.gzipClientVia(tcpTransfer, 10 * 1024 * 1024))
        .watchTermination()(Keep.right)
        .to(Sink.foreach {
          case x: ByteString ⇒
            x.toArray.grouped(TId.length).map(TId.keyFromBytes).foreach(tId ⇒ center.context.getContext[SourceContext](tId.persistenceId).actorRef ! tId)
          case x ⇒
            logger.warn(s"$x transfer errors")
        })
        .run()
      Await.result(future, 1.days)
      true
    } catch {
      case e: Exception ⇒ false
    }
  }
}