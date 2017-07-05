package teleporter.integration.cluster.instance

import java.net.InetAddress

import akka.actor.{Actor, ActorRef, Props}
import teleporter.integration.cluster.broker.PersistentProtocol.{Keys, Tables}
import teleporter.integration.cluster.instance.Brokers.{CreateConnection, _}
import teleporter.integration.cluster.rpc.EventBody.{ConfigChangeNotify, LinkInstance}
import teleporter.integration.cluster.rpc._
import teleporter.integration.cluster.rpc.fbs.{Action, MessageType, Role}
import teleporter.integration.core.TeleporterConfigActor._
import teleporter.integration.core.TeleporterContext.Remove
import teleporter.integration.core.{PartitionContext, TeleporterCenter}
import teleporter.integration.utils.{EventListener, SimpleHttpClient}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure, Random, Success}

/**
  * Created by kui.dai on 2016/8/5.
  */
class BrokersHolder(seedBrokers: String)(implicit center: TeleporterCenter) extends Actor with SimpleHttpClient {

  import center.materializer
  import context.{dispatcher, system}

  implicit val eventListener: EventListener[fbs.RpcMessage] = center.eventListener

  private var brokerConnections = Map[BrokerAddress, BrokerConnection]()

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    self ! LoaderBroker(seedBrokers)
  }

  override def receive: Receive = {
    case LoaderBroker(b) ⇒ loadBrokers(b)
    case CreateConnection(address) ⇒ createConnection(address)
    case SelectOne ⇒
      val selectBroker = brokerConnections.values.toSeq(Random.nextInt(brokerConnections.size))
      selectBroker.handler.request(MessageType.LinkInstance,
        LinkInstance(instance = center.instanceKey, broker = selectBroker.address.toString,
          ip = InetAddress.getLocalHost.getHostAddress, port = 9094, timestamp = System.currentTimeMillis()).toArray)
        .onComplete {
          case Success(_) ⇒ center.brokerConnection.success(selectBroker)
          case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
        }
  }

  def createConnection(address: BrokerAddress): Unit = {
    logger.info(s"Create connection for $address")
    var handler: RpcClientConnectionHandler = null
    handler = RpcClientConnectionHandler(address.ip, address.tcpPort, message ⇒ {
      message.role() match {
        case Role.Request ⇒
          message.messageType() match {
            case MessageType.ConfigChangeNotify ⇒
              val notify = RpcRequest.decode(message, EventBody.ConfigChangeNotify(_)).body.get
              notify.action match {
                case Action.ADD ⇒ upsertChanged(notify)
                case Action.UPDATE ⇒ upsertChanged(notify)
                case Action.UPSERT ⇒ upsertChanged(notify)
                case Action.REMOVE ⇒ center.context.ref ! Remove(center.context.getContext(notify.key))
              }
              handler.response(RpcResponse.success(message))
            case MessageType.LogTail ⇒ //todo
          }
        case Role.Response ⇒ center.eventListener.resolve(message.seqNr, message)
      }
    })
    handler.sourceQueue.watchCompletion().onComplete { r ⇒
      logger.warn(s"Connection $r was closed, will reconnect after 30.seconds")
      brokerConnections.get(address).foreach {
        brokerConnection ⇒
          brokerConnections -= address
          center.brokerConnection.future.onSuccess {
            case currBroker ⇒
              if (currBroker == brokerConnection) {
                center.brokerConnection = Promise[BrokerConnection]()
                self ! SelectOne
              }
          }
      }
      context.system.scheduler.scheduleOnce(30.seconds, self, CreateConnection(address))
    }
    brokerConnections += (address → BrokerConnection(address, handler))
    self ! SelectOne
  }

  def loadBrokers(brokers: String)(implicit ec: ExecutionContext): Unit = {
    brokers.split(",").map(_.split(":")).foreach {
      case Array(ip, tcpPort) ⇒ self ! CreateConnection(BrokerAddress(ip, tcpPort.toInt))
    }
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

  case class LoaderBroker(brokers: String)

  case class BrokerConnection(address: BrokerAddress, handler: RpcClientConnectionHandler)

  case class CreateConnection(address: BrokerAddress)

  case object SelectOne

  case class BrokerAddress(ip: String, tcpPort: Int)

  def apply(seedBrokers: String)(implicit center: TeleporterCenter): ActorRef = {
    center.system.actorOf(Props(classOf[BrokersHolder], seedBrokers, center))
  }
}