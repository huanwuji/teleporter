package teleporter.integration.template

import java.io.InputStream

import akka.Done
import akka.actor.ActorRef
import akka.stream._
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl._
import akka.util.ByteString
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component.tcp.TlsHelper
import teleporter.integration.core.{TId, TeleporterCenter}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Author: kui.dai
  * Date: 2016/4/18.
  */
trait TlsTcpAgent extends Logging {
  def agentStart[Out, M](serverBinding: Source[IncomingConnection, Future[ServerBinding]],
                         sink: ⇒ Graph[SinkShape[Out], M],
                         decoder: (ByteString, ActorRef) ⇒ scala.collection.immutable.Seq[Out],
                         batchSize: Int = 100,
                         password: String, keyStore: InputStream, trustStore: InputStream)(implicit center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    import TlsHelper._
    implicit val cipherSuites = defaultCipherSuites
    implicit val sslContext = initSslContext(password.toCharArray, keyStore, trustStore)
    import center.materializer
    implicit val executionContext = center.system.dispatcher

    serverBinding.viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both).to(Sink.foreach {
      connection ⇒
        logger.info(s"New connection from: ${connection.remoteAddress}")
        var sourceRef: ActorRef = ActorRef.noSender
        Source.actorRef[TId](100000, OverflowStrategy.fail)
          .mapMaterializedValue { ref ⇒ sourceRef = ref; ref }
          .groupedWithin(batchSize, 30.seconds)
          .map(_.flatMap(_.toBytes).toArray).map(ByteString(_))
          .via(TlsHelper.gzipServerVia(connection, 10 * 1024 * 1024))
          .filter { b ⇒ if (b.isEmpty) logger.info("Heartbeat"); b.nonEmpty }
          .mapConcat[Out](x ⇒ decoder(x, sourceRef))
          .to(sink).run()
    }).run()
  }
}