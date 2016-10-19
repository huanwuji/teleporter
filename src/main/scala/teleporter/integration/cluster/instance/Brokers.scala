package teleporter.integration.cluster.instance

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Actor, ActorRef, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Framing, Keep, Sink, Source, Tcp}
import akka.util.ByteString
import teleporter.integration.cluster.broker.PersistentProtocol.Values.BrokerValue
import teleporter.integration.cluster.broker.PersistentProtocol.{KeyBean, KeyValue, Keys, Tables}
import teleporter.integration.cluster.instance.Brokers.{CreateConnection, _}
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.TeleporterRpc
import teleporter.integration.cluster.rpc.proto.TeleporterRpc._
import teleporter.integration.cluster.rpc.proto.broker.Broker.LinkInstance
import teleporter.integration.cluster.rpc.proto.instance.Instance.ConfigChangeNotify
import teleporter.integration.core.TeleporterContext.Remove
import teleporter.integration.core.{TeleporterCenter, TeleporterConfigService}
import teleporter.integration.utils.SimpleHttpClient

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

/**
  * Created by kui.dai on 2016/8/5.
  */
class Brokers()(implicit center: TeleporterCenter) extends Actor with SimpleHttpClient {

  import center.materializer
  import context.{dispatcher, system}

  private var brokerConnections = Map[String, BrokerConnection]()
  private var brokerLeader: BrokerConnection = _

  override def receive: Receive = {
    case LoaderBroker(b) ⇒ loadBrokers(b)
    case CreateConnection(broker) ⇒
      val receiverRef = context.actorOf(Props(classOf[BrokerEventReceiverActor], center))
      val (senderRef, fu) = Source.actorRef[TeleporterEvent](100, OverflowStrategy.fail)
        .map { m ⇒
          println(
            s"""
               |33333333333333
               |${m.getSeqNr}
               |${m.getRole}
               |${m.getStatus}
               |${m.getType}
               |${m.getBody.toStringUtf8}
            """.stripMargin)
          ByteString(m.toByteArray)
        }
        .watchTermination()(Keep.both)
        .merge(Source.tick(30.seconds, 30.seconds, ByteString()))
        .via(Framing.simpleFramingProtocol(10 * 1024 * 1024)
          .join(Tcp().outgoingConnection(remoteAddress = InetSocketAddress.createUnresolved(broker.value.ip, broker.value.tcpPort), connectTimeout = 2.minutes, idleTimeout = 2.minutes)))
        .map(bs ⇒ {
          val aa = TeleporterEvent.parseFrom(bs.toArray)
          println(
            s"""
               |44444444444444444
               |${aa.getSeqNr}
               |${aa.getRole}
               |${aa.getStatus}
               |${aa.getType}
               |${aa.getBody.toStringUtf8}
             """.stripMargin)
          aa
        })
        .to(Sink.actorRef(receiverRef, Complete)).run()
      receiverRef ! RegisterSender(senderRef)
      fu.onComplete {
        r ⇒
          logger.warn(s"Connection was closed, $r")
          ReConnection(broker)
      }
      brokerConnections += (broker.key → BrokerConnection(senderRef, receiverRef, broker))
      self ! SelectLeader
    case ReConnection(broker) ⇒
      brokerConnections -= broker.key
      self ! CreateConnection(broker)
      self ! SelectLeader
    case SelectLeader ⇒
      brokerLeader = brokerConnections.values.toSeq(Random.nextInt(brokerConnections.size))
      center.eventListener.asyncEvent { seqNr ⇒
        brokerLeader.senderRef ! TeleporterEvent.newBuilder()
          .setSeqNr(seqNr)
          .setRole(TeleporterEvent.Role.CLIENT)
          .setType(EventType.LinkInstance)
          .setBody(
            LinkInstance.newBuilder()
              .setInstance(center.instance)
              .setBroker(brokerLeader.broker.key)
              .setIp(InetAddress.getLocalHost.getHostAddress)
              //              .setPort(center.port)
              .setTimestamp(System.currentTimeMillis())
              .build().toByteString
          ).build()
      }
    case SendMessage(event) ⇒
      brokerLeader.senderRef ! event
  }

  def loadBrokers(brokers: String)(implicit ec: ExecutionContext): Unit = {
    val seedBrokerServers = brokers.split(",").map(_.split(":")).map {
      case Array(ip, port) ⇒ Http().outgoingConnection(ip, port.toInt)
    }.toSeq
    Random.shuffle(seedBrokerServers).foreach { implicit server ⇒
      simpleRequest[Seq[KeyValue]](RequestBuilding.Get(s"/config/range?key=${Keys.mapping(center.instance, Keys.INSTANCE, Keys.BROKERS)}")).onComplete {
        case Success(kvs) ⇒
          kvs.map(_.keyBean[BrokerValue]).foreach { bv ⇒
            implicit val server = Http().outgoingConnection(bv.value.ip, bv.value.port)
            simpleRequest[String](RequestBuilding.Get("/ping")).onComplete {
              case Success(_) ⇒ self ! CreateConnection(bv)
              case Failure(e) ⇒
                logger.error(e.getLocalizedMessage, e)
                self ! LoaderBroker
            }
          }
        case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
      }
    }
  }
}

class BrokerEventReceiverActor()(implicit val center: TeleporterCenter) extends Actor {

  import context.dispatcher

  var configService: TeleporterConfigService = _
  var senderRef: ActorRef = _
  val logTrace = context.actorOf(Props(classOf[LogTrace], center))

  override def receive: Receive = {
    case Status.Failure(cause) ⇒
    case Complete ⇒ throw new RuntimeException("Error, Connection is forever!")
    case RegisterSender(ref) ⇒
      this.senderRef = ref
      this.configService = TeleporterConfigService(center.eventListener)
    case event: TeleporterEvent ⇒
      if (event.getRole == TeleporterEvent.Role.CLIENT) {
        center.eventListener.resolve(event.getSeqNr, event)
      } else {
        (eventHandle: EventHandle) ((event.getType, event))
      }
  }

  def eventHandle: EventHandle = {
    case (EventType.ConfigChangeNotify, event) ⇒
      val notify = ConfigChangeNotify.parseFrom(event.getBody)
      notify.getAction match {
        case ConfigChangeNotify.Action.ADD ⇒ upsertChanged(notify)
        case ConfigChangeNotify.Action.UPDATE ⇒ upsertChanged(notify)
        case ConfigChangeNotify.Action.UPSERT ⇒ upsertChanged(notify)
        case ConfigChangeNotify.Action.REMOVE ⇒ center.context.ref ! Remove(center.context.getContext(notify.getKey))
      }
      senderRef ! TeleporterRpc.success(event)
    case (EventType.LogRequest, event) ⇒
      logTrace ! event
  }

  def upsertChanged(notify: ConfigChangeNotify) = {
    val key = notify.getKey
    Keys.table(key) match {
      case Tables.partition ⇒ configService.loadPartition(key)
      case Tables.stream ⇒ configService.loadStream(key)
      case Tables.task ⇒ configService.loadTask(key)
      case Tables.source ⇒ configService.loadSource(key)
      case Tables.sink ⇒ configService.loadSink(key)
      case Tables.address ⇒ configService.loadAddress(key)
    }
  }
}

object Brokers {

  case object Complete

  case class SendMessage(event: TeleporterEvent)

  case class LoaderBroker(brokers: String)

  case class BrokerConnection(senderRef: ActorRef, receiveRef: ActorRef, broker: KeyBean[BrokerValue])

  case class ReConnection(broker: KeyBean[BrokerValue])

  case class CreateConnection(broker: KeyBean[BrokerValue])

  case object SelectLeader

  case class RegisterSender(ref: ActorRef)

  def apply()(implicit center: TeleporterCenter) = {
    center.system.actorOf(Props(classOf[Brokers], center))
  }
}