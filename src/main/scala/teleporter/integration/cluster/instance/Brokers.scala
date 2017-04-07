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
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.broker.PersistentProtocol.Values.BrokerValue
import teleporter.integration.cluster.broker.PersistentProtocol.{KeyBean, KeyValue, Keys, Tables}
import teleporter.integration.cluster.instance.Brokers.{CreateConnection, _}
import teleporter.integration.cluster.rpc.EventBody.{ConfigChangeNotify, LinkInstance}
import teleporter.integration.cluster.rpc.TeleporterEvent.EventHandle
import teleporter.integration.cluster.rpc.fbs.{Action, EventType, Role}
import teleporter.integration.cluster.rpc.{EventBody, TeleporterEvent}
import teleporter.integration.core.TeleporterConfigActor._
import teleporter.integration.core.TeleporterContext.Remove
import teleporter.integration.core.{PartitionContext, TeleporterCenter}
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
      logger.info(s"Create connection for $broker")
      val receiverRef = context.actorOf(Props(classOf[BrokerEventReceiverActor], center))
      val (senderRef, fu) = Source.actorRef[TeleporterEvent[_ <: EventBody]](100, OverflowStrategy.fail)
        .log("client-send")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .map(event ⇒ ByteString(TeleporterEvent.toArray(event)))
        .watchTermination()(Keep.both)
        .merge(Source.tick(30.seconds, 30.seconds, ByteString()))
        .via(Framing.simpleFramingProtocol(10 * 1024 * 1024)
          .join(Tcp().outgoingConnection(remoteAddress = InetSocketAddress.createUnresolved(broker.value.ip, broker.value.tcpPort), connectTimeout = 2.minutes, idleTimeout = 2.minutes)))
        .map(TeleporterEvent(_))
        .log("client-receiver")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .to(Sink.actorRef(receiverRef, Complete)).run()
      receiverRef ! RegisterSender(senderRef)
      fu.onComplete {
        r ⇒
          logger.warn(s"Connection $r was closed, will reconnect after 30.seconds")
          brokerConnections.get(broker.key).foreach {
            brokerConnection: BrokerConnection ⇒
              if (brokerConnection == mainBroker) {
                mainBroker = null
              }
          }
          brokerConnections -= broker.key
          context.system.scheduler.scheduleOnce(30.seconds, self, CreateConnection(broker))
      }
      brokerConnections += (broker.key → BrokerConnection(senderRef, receiverRef, broker))
      self ! SelectOne
    case SelectOne ⇒
      mainBroker = brokerConnections.values.toSeq(Random.nextInt(brokerConnections.size))
      center.eventListener.asyncEvent { seqNr ⇒
        mainBroker.senderRef ! TeleporterEvent.request(seqNr = seqNr, eventType = EventType.LinkInstance,
          body = LinkInstance(instance = center.instanceKey, broker = mainBroker.broker.key,
            ip = InetAddress.getLocalHost.getHostAddress, port = 9094, timestamp = System.currentTimeMillis()))
      }._2.onComplete {
        case Success(_) ⇒
          if (!connected.isCompleted) {
            connected.trySuccess(Done)
          }
        case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
      }
    case SendMessage(event) ⇒ if (mainBroker != null) mainBroker.senderRef ! event
  }

  def loadBrokers(brokers: String)(implicit ec: ExecutionContext): Unit = {
    val seedBrokerServers = brokers.split(",").map(_.split(":")).map {
      case Array(ip, port) ⇒ Http().outgoingConnection(ip, port.toInt)
    }.toSeq
    loadBrokers(Random.shuffle(seedBrokerServers))
  }

  def loadBrokers(seedBrokerServers: Seq[Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]]], idx: Int = 0)(implicit ec: ExecutionContext): Unit = {
    implicit val server = seedBrokerServers(idx)
    val brokersKey = s"/config/range?key=${Keys.mapping(center.instanceKey, Keys.INSTANCE, Keys.BROKERS)}"
    simpleRequest[Seq[KeyValue]](RequestBuilding.Get(brokersKey))
      .onComplete {
        case Success(kvs) ⇒
          kvs.map(_.keyBean[BrokerValue]) match {
            case Nil ⇒
              logger.warn(s"Can't find brokers from $brokersKey, Please config in the ui")
            case _ ⇒
              kvs.map(_.keyBean[BrokerValue]).foreach { bv ⇒
                implicit val server = Http().outgoingConnection(bv.value.ip, bv.value.port)
                simpleRequest[String](RequestBuilding.Get("/ping")).onComplete {
                  case Success(_) ⇒ self ! CreateConnection(bv)
                  case Failure(e) ⇒ logger.error(s"$bv ping failure, ${e.getLocalizedMessage}", e)
                }
              }
          }
        case Failure(e) ⇒
          logger.error(e.getLocalizedMessage, e)
          if (idx + 1 < seedBrokerServers.size) {
            loadBrokers(seedBrokerServers, idx + 1)
          } else {
            logger.warn("Can't connect any broker, will retry after 1.minute")
            context.system.scheduler.scheduleOnce(1.minute, self, LoaderBroker)
          }
      }
  }
}

class BrokerEventReceiverActor()(implicit val center: TeleporterCenter) extends Actor with Logging {

  var senderRef: ActorRef = _

  override def receive: Receive = {
    case Status.Failure(cause) ⇒ logger.error(cause.getLocalizedMessage, cause)
    case Complete ⇒ throw new RuntimeException("Error, Connection is forever!")
    case RegisterSender(ref) ⇒
      this.senderRef = ref
    case event: TeleporterEvent[_] if event.role == Role.Request ⇒
      (eventHandle: EventHandle) ((event.eventType, event))
    case event: TeleporterEvent[_] if event.role == Role.Response ⇒
      center.eventListener.resolve(event.seqNr, event)
  }

  def eventHandle: EventHandle = {
    case (EventType.ConfigChangeNotify, event) ⇒
      val notify = event.toBody[ConfigChangeNotify]
      notify.action match {
        case Action.ADD ⇒ upsertChanged(notify)
        case Action.UPDATE ⇒ upsertChanged(notify)
        case Action.UPSERT ⇒ upsertChanged(notify)
        case Action.REMOVE ⇒ center.context.ref ! Remove(center.context.getContext(notify.key))
      }
      senderRef ! TeleporterEvent.success(event)
    case (EventType.LogTail, event) ⇒ //todo
  }

  def errorLog: PartialFunction[Throwable, Unit] = {
    case e: Throwable ⇒ logger.error(e.getMessage, e)
  }

  def upsertChanged(notify: ConfigChangeNotify): Unit = {
    val key = notify.key
    val config = center.configRef
    Keys.table(key) match {
      case Tables.partition ⇒ config ! LoadPartition(key)
      case Tables.stream ⇒ config ! LoadStream(key)
      case Tables.task ⇒
        center.context.indexes.rangeTo[PartitionContext](Keys.mapping(key, Keys.TASK, Keys.PARTITIONS))
          .foreach { case (k, _) ⇒ config ! LoadPartition(k) }
      case Tables.source ⇒ config ! LoadSource(key)
      case Tables.sink ⇒ config ! LoadSink(key)
      case Tables.address ⇒ config ! LoadAddress(key)
    }
  }
}

object Brokers {

  case object Complete

  case class SendMessage[T <: EventBody](event: TeleporterEvent[T])

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