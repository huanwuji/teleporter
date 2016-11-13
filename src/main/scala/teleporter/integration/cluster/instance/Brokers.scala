package teleporter.integration.cluster.instance

import java.net.{InetAddress, InetSocketAddress}

import akka.Done
import akka.actor.{Actor, ActorRef, Props, Status}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source, Tcp}
import akka.stream.{Attributes, OverflowStrategy}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.cluster.broker.PersistentProtocol.Values.BrokerValue
import teleporter.integration.cluster.broker.PersistentProtocol.{KeyBean, KeyValue, Keys, Tables}
import teleporter.integration.cluster.instance.Brokers.{CreateConnection, _}
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.TeleporterRpc
import teleporter.integration.cluster.rpc.proto.TeleporterRpc._
import teleporter.integration.cluster.rpc.proto.broker.Broker.LinkInstance
import teleporter.integration.cluster.rpc.proto.instance.Instance.ConfigChangeNotify
import teleporter.integration.core.TeleporterCenter
import teleporter.integration.core.TeleporterConfigActor.{apply ⇒ _, _}
import teleporter.integration.core.TeleporterContext.Remove
import teleporter.integration.utils.SimpleHttpClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

/**
  * Created by kui.dai on 2016/8/5.
  */
class Brokers(seedBrokers: String, connected: Promise[Done])(implicit center: TeleporterCenter) extends Actor with SimpleHttpClient {

  import center.materializer
  import context.{dispatcher, system}

  private var brokerConnections = Map[String, BrokerConnection]()
  private var mainBroker: BrokerConnection = _


  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    self ! LoaderBroker(seedBrokers)
  }

  override def receive: Receive = {
    case LoaderBroker(b) ⇒
      loadBrokers(b)
    case CreateConnection(broker) ⇒
      val receiverRef = context.actorOf(Props(classOf[BrokerEventReceiverActor], center))
      val (senderRef, fu) = Source.actorRef[TeleporterEvent](100, OverflowStrategy.fail)
        .log("client-send")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .map(m ⇒ ByteString(m.toByteArray))
        .watchTermination()(Keep.both)
        .merge(Source.tick(30.seconds, 30.seconds, ByteString()))
        .via(Framing.simpleFramingProtocol(10 * 1024 * 1024)
          .join(Tcp().outgoingConnection(remoteAddress = InetSocketAddress.createUnresolved(broker.value.ip, broker.value.tcpPort), connectTimeout = 2.minutes, idleTimeout = 2.minutes)))
        .map(bs ⇒ TeleporterEvent.parseFrom(bs.toArray))
        .log("client-receiver")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .to(Sink.actorRef(receiverRef, Complete)).run()
      receiverRef ! RegisterSender(senderRef)
      fu.onComplete {
        r ⇒
          logger.warn(s"Connection was closed, $r")
          brokerConnections -= broker.key
          self ! CreateConnection(broker)
          self ! SelectOne
      }
      brokerConnections += (broker.key → BrokerConnection(senderRef, receiverRef, broker))
      self ! SelectOne
    case SelectOne ⇒
      mainBroker = brokerConnections.values.toSeq(Random.nextInt(brokerConnections.size))
      center.eventListener.asyncEvent { seqNr ⇒
        mainBroker.senderRef ! TeleporterEvent.newBuilder()
          .setSeqNr(seqNr)
          .setRole(TeleporterEvent.Role.CLIENT)
          .setType(EventType.LinkInstance)
          .setBody(
            LinkInstance.newBuilder()
              .setInstance(center.instanceKey)
              .setBroker(mainBroker.broker.key)
              .setIp(InetAddress.getLocalHost.getHostAddress)
              //              .setPort(center.port)
              .setTimestamp(System.currentTimeMillis())
              .build().toByteString
          ).build()
      }._2.onComplete {
        case Success(_) ⇒
          if (!connected.isCompleted) {
            connected.trySuccess(Done)
          }
        case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
      }
    case SendMessage(event) ⇒ mainBroker.senderRef ! event
  }

  def loadBrokers(brokers: String)(implicit ec: ExecutionContext): Unit = {
    val seedBrokerServers = brokers.split(",").map(_.split(":")).map {
      case Array(ip, port) ⇒ Http().outgoingConnection(ip, port.toInt)
    }.toSeq
    Random.shuffle(seedBrokerServers).foreach { implicit server ⇒
      simpleRequest[Seq[KeyValue]](RequestBuilding.Get(s"/config/range?key=${Keys.mapping(center.instanceKey, Keys.INSTANCE, Keys.BROKERS)}")).onComplete {
        case Success(kvs) ⇒
          kvs.map(_.keyBean[BrokerValue]).foreach { bv ⇒
            implicit val server = Http().outgoingConnection(bv.value.ip, bv.value.port)
            simpleRequest[String](RequestBuilding.Get("/ping")).onComplete {
              case Success(_) ⇒ self ! CreateConnection(bv)
              case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
            }
          }
        case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
      }
    }
  }

  def loadBrokers(seedBrokerServers: Seq[Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]]], idx: Int = 0)(implicit ec: ExecutionContext): Unit = {
    implicit val server = seedBrokerServers(idx)
    simpleRequest[Seq[KeyValue]](RequestBuilding.Get(s"/config/range?key=${Keys.mapping(center.instanceKey, Keys.INSTANCE, Keys.BROKERS)}"))
      .onComplete {
        case Success(kvs) ⇒
          kvs.map(_.keyBean[BrokerValue]).foreach { bv ⇒
            implicit val server = Http().outgoingConnection(bv.value.ip, bv.value.port)
            simpleRequest[String](RequestBuilding.Get("/ping")).onComplete {
              case Success(_) ⇒ self ! CreateConnection(bv)
              case Failure(e) ⇒ logger.error(s"$bv ping failure, ${e.getLocalizedMessage}", e)
            }
          }
        case Failure(e) ⇒
          if (idx + 1 < seedBrokerServers.size) {
            loadBrokers(seedBrokerServers, idx + 1)
          } else {
            logger.error("Can't load any brokers")
          }
      }
  }
}

class BrokerEventReceiverActor()(implicit val center: TeleporterCenter) extends Actor with LazyLogging {

  var senderRef: ActorRef = _
  val logTrace = context.actorOf(Props(classOf[LogTrace], center))

  override def receive: Receive = {
    case Status.Failure(cause) ⇒
    case Complete ⇒ throw new RuntimeException("Error, Connection is forever!")
    case RegisterSender(ref) ⇒
      this.senderRef = ref
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

  def errorLog: PartialFunction[Throwable, Unit] = {
    case e: Throwable ⇒ logger.error(e.getMessage, e)
  }

  def upsertChanged(notify: ConfigChangeNotify) = {
    val key = notify.getKey
    val config = center.configRef
    Keys.table(key) match {
      case Tables.partition ⇒ config ! LoadPartition(key)
      case Tables.stream ⇒ config ! LoadStream(key)
      case Tables.task ⇒ config ! LoadTask(key)
      case Tables.source ⇒ config ! LoadSource(key)
      case Tables.sink ⇒ config ! LoadSink(key)
      case Tables.address ⇒ config ! LoadAddress(key)
    }
  }
}

object Brokers {

  case object Complete

  case class SendMessage(event: TeleporterEvent)

  case class LoaderBroker(brokers: String)

  case class BrokerConnection(senderRef: ActorRef, receiveRef: ActorRef, broker: KeyBean[BrokerValue])

  case class CreateConnection(broker: KeyBean[BrokerValue])

  case object SelectOne

  case class RegisterSender(ref: ActorRef)

  def apply(seedBrokers: String)(implicit center: TeleporterCenter): (ActorRef, Future[Done]) = {
    val connected = Promise[Done]()
    (center.system.actorOf(Props(classOf[Brokers], seedBrokers, connected, center)), connected.future)
  }
}