package teleporter.integration.template

import java.io.InputStream

import akka.Done
import akka.stream._
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.util.ByteString
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component.tcp.TlsHelper
import teleporter.integration.core.{SourceContext, TId, TeleporterCenter}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Author: kui.dai
  * Date: 2016/4/18.
  */
trait TlsTcpClient extends Logging {
  def clientStart[T, M](sourceShape: Graph[SourceShape[T], M],
                        outgoing: Flow[ByteString, ByteString, Future[OutgoingConnection]],
                        batchSize: Int = 100,
                        encoder: Seq[T] ⇒ ByteString,
                        password: String, keyStore: InputStream, trustStore: InputStream)(implicit center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    import TlsHelper._
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
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .to(Sink.foreach {
        case x: ByteString ⇒
          x.toArray.grouped(TId.length).map(TId.keyFromBytes).foreach(tId ⇒ center.context.getContext[SourceContext](tId.persistenceId).actorRef ! tId)
        case x ⇒
          logger.warn(s"$x transfer errors")
      })
      .run()
  }
}