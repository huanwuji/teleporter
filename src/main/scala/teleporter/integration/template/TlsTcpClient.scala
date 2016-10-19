package teleporter.integration.template

import java.io.InputStream

import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.stream.{Graph, SourceShape, TLSClosing}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.tcp.TlsHelper
import teleporter.integration.core.TeleporterConfig.StreamConfig
import teleporter.integration.core.{SourceContext, TId, TeleporterCenter}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Author: kui.dai
  * Date: 2016/4/18.
  */
trait TlsTcpClient extends LazyLogging {
  def clientStart[T, M](sourceShape: Graph[SourceShape[T], M],
                        outgoing: Flow[ByteString, ByteString, Future[OutgoingConnection]],
                        terminationHandler: ⇒ Unit,
                        batchSize: Int = 100,
                        encoder: Seq[T] ⇒ ByteString,
                        password: String, keyStore: InputStream, trustStore: InputStream)(implicit center: TeleporterCenter): Any = {
    import TlsHelper._

    try {
      implicit val cipherSuites = defaultCipherSuites
      implicit val sslContext = initSslContext(password.toCharArray, keyStore, trustStore)
      import center.materializer
      implicit val executionContext = center.system.dispatcher

      def tlsOutgoingFlow = clientTls(TLSClosing.eagerClose) join outgoing

      Source.fromGraph(sourceShape)
        .grouped(batchSize)
        .map(encoder)
        .merge(Source.tick(30.seconds, 30.seconds, ByteString()))
        .via(TlsHelper.gzipClientVia(tlsOutgoingFlow, 10 * 1024 * 1024))
        .watchTermination()(Keep.right)
        .to(Sink.foreach {
          case x: ByteString ⇒
            x.toArray.grouped(TId.length).map(TId.keyFromBytes).foreach(tId ⇒ center.context.getContext[SourceContext](tId.persistenceId).actorRef ! tId)
          case x ⇒
            logger.warn(s"$x transfer errors")
        })
        .run().onComplete {
        case x ⇒
          logger.info(x.toString)
          terminationHandler
      }
    } catch {
      case e: Exception ⇒ logger.error(e.getLocalizedMessage, e)
    }
  }

  def clientStop(stream: StreamConfig)(implicit center: TeleporterCenter): Future[Boolean] = {
    //todo
    Future.successful(true)
  }

  //  {
  //    implicit val ec = center.system.dispatcher
  //    val props: MapBean = stream.props
  //    val source = center.context.getContext[SourceContext](props[String]("source"))
  //    val sourceRef = if (source.isShadow) center.shadowActor(source.id) else center.actor(source.id)
  //    gracefulStop(sourceRef, 2.minutes, CompleteThenStop)
  //  }
}