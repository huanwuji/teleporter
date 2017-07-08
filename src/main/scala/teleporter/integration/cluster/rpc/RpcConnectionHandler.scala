package teleporter.integration.cluster.rpc

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream._
import akka.stream.scaladsl.{Framing, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.util.ByteString
import teleporter.integration.cluster.rpc.fbs.{MessageStatus, MessageType, Role}
import teleporter.integration.utils.EventListener

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by huanwuji on 2017/4/10.
  */
trait RpcConnectionHandler {
  def sourceQueue: SourceQueueWithComplete[ByteString]

  def send(bs: ByteString): Future[QueueOfferResult] = sourceQueue.offer(bs)

  def response(bs: ByteString): Future[QueueOfferResult] = send(bs)

  def request(eventType: Byte, array: Array[Byte])(implicit eventListener: EventListener[fbs.RpcMessage]): Future[fbs.RpcMessage] = {
    eventListener.asyncEvent { seqNr ⇒
      send(RpcRequest.encodeByteString(seqNr, eventType, array))
    }._2
  }
}

case class RpcServerConnectionHandler(connection: Tcp.IncomingConnection, sourceQueue: SourceQueueWithComplete[ByteString]) extends RpcConnectionHandler

object RpcServerConnectionHandler {
  def apply(connection: Tcp.IncomingConnection, handle: fbs.RpcMessage ⇒ Unit)(implicit mater: Materializer): RpcServerConnectionHandler = {
    new RpcServerConnectionHandler(
      connection = connection,
      sourceQueue = Source.queue[ByteString](100, OverflowStrategy.fail)
        .log("server-send", bs ⇒ {
          val m = RpcMessage.decode(bs)
          s"""seqNr:${m.seqNr()},
             |messageType:${MessageType.name(m.messageType())},
             |role:${Role.name(m.role())},
             |status:${MessageStatus.name(m.status())},
             |body:${new String(Array.tabulate(m.bodyLength())(m.body))}""".stripMargin
        })
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .via(Framing.simpleFramingProtocol(10 * 1024 * 1024).join(connection.flow))
        .filter(_.nonEmpty)
        .map(RpcMessage.decode)
        .log("server-receiver", m ⇒
          s"""seqNr:${m.seqNr()},
             |messageType:${MessageType.name(m.messageType())},
             |role:${Role.name(m.role())},
             |status:${MessageStatus.name(m.status())},
             |body:${new String(Array.tabulate(m.bodyLength())(m.body))}""".stripMargin)
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .to(Sink.foreach(handle)).run()
    )
  }
}

case class RpcClientConnectionHandler(sourceQueue: SourceQueueWithComplete[ByteString]) extends RpcConnectionHandler

object RpcClientConnectionHandler {
  def apply(ip: String, port: Int, handle: fbs.RpcMessage ⇒ Unit)
           (implicit system: ActorSystem, mater: Materializer): RpcClientConnectionHandler = {
    new RpcClientConnectionHandler(
      sourceQueue = Source.queue[ByteString](100, OverflowStrategy.fail)
        .log("client-send", bs ⇒ {
          val m = RpcMessage.decode(bs)
          s"""seqNr:${m.seqNr()},
             |messageType:${MessageType.name(m.messageType())},
             |role:${Role.name(m.role())},
             |status:${MessageStatus.name(m.status())},
             |body:${new String(Array.tabulate(m.bodyLength())(m.body))}""".stripMargin
        })
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .merge(Source.tick(30.seconds, 30.seconds, ByteString()))
        .via(Framing.simpleFramingProtocol(10 * 1024 * 1024)
          .join(Tcp().outgoingConnection(remoteAddress = InetSocketAddress.createUnresolved(ip, port), connectTimeout = 2.minutes, idleTimeout = 2.minutes)))
        .map(RpcMessage.decode)
        .log("client-receiver", m ⇒
          s"""seqNr:${m.seqNr()},
             |messageType:${MessageType.name(m.messageType())},
             |role:${Role.name(m.role())},
             |status:${MessageStatus.name(m.status())},
             |body:${new String(Array.tabulate(m.bodyLength())(m.body))}""".stripMargin)
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .to(Sink.foreach(handle)).run()
    )
  }
}
