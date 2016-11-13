package teleporter.integration.cluster.broker.tcp

import akka.actor.{Actor, ActorRef, Props, Status}
import akka.event.Logging
import akka.stream.scaladsl.{Framing, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, Attributes, OverflowStrategy}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.cluster.broker.PersistentProtocol.Keys
import teleporter.integration.cluster.broker.PersistentProtocol.Values.{toString ⇒ _, _}
import teleporter.integration.cluster.broker.PersistentService
import teleporter.integration.cluster.broker.tcp.EventReceiveActor._
import teleporter.integration.cluster.rpc.proto.Rpc.{TeleporterEvent, _}
import teleporter.integration.cluster.rpc.proto.TeleporterRpc
import teleporter.integration.cluster.rpc.proto.TeleporterRpc.EventHandle
import teleporter.integration.cluster.rpc.proto.broker.Broker._
import teleporter.integration.cluster.rpc.proto.instance.Instance.ConfigChangeNotify
import teleporter.integration.utils.EventListener

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by kui.dai on 2016/7/26.
  */
case class ConnectionKeeper(senderRef: ActorRef, receiverRef: ActorRef)

class EventReceiveActor(configService: PersistentService,
                        runtimeService: PersistentService,
                        connectionKeepers: TrieMap[String, ConnectionKeeper],
                        eventListener: EventListener[TeleporterEvent])
  extends Actor with LazyLogging {

  import context.dispatcher
  import teleporter.integration.cluster.broker.PersistentProtocol.Keys._

  var senderRef: ActorRef = _
  var currInstance: String = _

  override def receive: Receive = {
    case Status.Failure(cause) ⇒
      self ! InstanceOffline(this.currInstance)
      connectionKeepers -= this.currInstance
    case Complete ⇒
      self ! InstanceOffline(this.currInstance)
      throw new RuntimeException("Error, Connection can't closed!")
    case RegisterSender(ref) ⇒ senderRef = ref
    case StartPartition(partition, instance) ⇒
      runtimeService.put(partition, Version().toJson)
      connectionKeepers.get(instance).foreach { keeper ⇒
        eventListener.asyncEvent { seqNr ⇒
          senderRef ! TeleporterEvent.newBuilder()
            .setSeqNr(seqNr)
            .setRole(TeleporterEvent.Role.SERVER)
            .setType(EventType.ConfigChangeNotify)
            .setBody(
              ConfigChangeNotify.newBuilder().setKey(partition).setAction(ConfigChangeNotify.Action.UPSERT).setTimestamp(System.currentTimeMillis()).build().toByteString
            ).build()
        }._2.onComplete {
          case Success(_) ⇒
            runtimeService.put(Keys.mapping(partition, PARTITION, RUNTIME_PARTITION),
              RuntimePartitionValue(instance = instance, status = InstanceStatus.online, timestamp = System.currentTimeMillis()).toJson)
          case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
        }
      }
    case StopPartition(partition, instance) ⇒
      runtimeService.delete(partition)
      connectionKeepers.get(instance).foreach { keeper ⇒
        eventListener.asyncEvent { seqNr ⇒
          senderRef ! TeleporterEvent.newBuilder()
            .setSeqNr(seqNr)
            .setRole(TeleporterEvent.Role.SERVER)
            .setType(EventType.ConfigChangeNotify)
            .setBody(
              ConfigChangeNotify.newBuilder().setKey(partition).setAction(ConfigChangeNotify.Action.REMOVE).setTimestamp(System.currentTimeMillis()).build().toByteString
            ).build()
        }._2.onComplete {
          case Success(_) ⇒ runtimeService.delete(Keys.mapping(partition, PARTITION, RUNTIME_PARTITION))
          case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
        }
      }
    case InstanceOffline(instance) ⇒
      //offline instance
      runtimeService.get(instance).map(_.keyBean[RuntimeInstanceValue]).foreach {
        kb ⇒ runtimeService.atomicPut(instance, kb.value.toJson, kb.value.copy(status = InstanceStatus.offline, timestamp = System.currentTimeMillis()).toJson)
      }
      //offline instance partition
      configService.get(instance).map(_.keyBean[InstanceValue])
        .flatMap(instanceKV ⇒ configService.get(instanceKV.value.group).map(_.keyBean[GroupValue]))
        .foreach {
          groupKV ⇒
            Duration(groupKV.value.instanceOfflineReBalanceTime) match {
              case d: FiniteDuration ⇒ context.system.scheduler.scheduleOnce(d, self, ReBalance(groupKV.key))
              case _ ⇒
            }
            groupKV.value.tasks.flatMap(taskKey ⇒ runtimeService.range(Keys.mapping(taskKey, TASK, RUNTIME_PARTITIONS)))
              .map(_.keyBean[RuntimePartitionValue])
              .foreach {
                kb ⇒ runtimeService.atomicPut(kb.key, kb.value.toJson, kb.value.copy(status = InstanceStatus.offline).toJson)
              }
        }
    case ReBalance(group, instance) ⇒ reBalance(group, instance)
    case event: TeleporterEvent ⇒
      eventHandle.orElse(configEventHandle).orElse(defaultEventHandle).apply((event.getType, event))
  }

  def eventHandle: EventHandle = {
    case (EventType.LinkInstance, event) ⇒
      val li = LinkInstance.parseFrom(event.getBody)
      val instance = configService(li.getInstance()).keyBean[InstanceValue]
      this.currInstance = instance.key
      connectionKeepers += this.currInstance → ConnectionKeeper(senderRef, self)
      runtimeService.put(
        key = Keys.mapping(li.getInstance(), INSTANCE, RUNTIME_INSTANCE),
        value = RuntimeInstanceValue(
          ip = li.getIp,
          port = li.getPort,
          status = InstanceStatus.online,
          broker = li.getBroker,
          timestamp = li.getTimestamp).toJson
      )
      self ! ReBalance(instance.value.group, Some(li.getInstance()))
      senderRef ! TeleporterRpc.success(event)
    case (EventType.LinkAddress, event) ⇒
      val ln = LinkAddress.parseFrom(event.getBody)
      runtimeService.put(
        key = Keys(RUNTIME_ADDRESS, Keys.unapply(ln.getAddress, ADDRESS) ++ Keys.unapply(ln.getInstance(), INSTANCE)),
        value = RuntimeAddressValue(keys = ln.getKeysList.asScala.toSet, timestamp = ln.getTimestamp).toJson
      )
      senderRef ! TeleporterRpc.success(event)
    case (EventType.LinkVariable, event) ⇒
      val ln = LinkVariable.parseFrom(event.getBody)
      runtimeService.put(
        key = Keys(RUNTIME_VARIABLE, Keys.unapply(ln.getVariableKey, VARIABLE) ++ Keys.unapply(ln.getInstance(), INSTANCE)),
        value = RuntimeVariableValue(ln.getKeysList.asScala.toSet, timestamp = ln.getTimestamp).toJson
      )
      senderRef ! TeleporterRpc.success(event)
    case (EventType.LogResponse, event) ⇒
      eventListener.resolve(event.getSeqNr, event)
  }

  def configEventHandle: EventHandle = {
    case (EventType.KVGet, event) ⇒
      val get = KVGet.parseFrom(event.getBody)
      val kv = configService(get.getKey)
      senderRef ! TeleporterEvent.newBuilder(event)
        .setBody(KV.newBuilder().setKey(kv.key).setValue(kv.value).build().toByteString)
        .build()
    case (EventType.RangeRegexKV, event) ⇒
      val rangeKV = RangeRegexKV.parseFrom(event.getBody)
      val kvs = configService.regexRange(rangeKV.getKey, rangeKV.getStart, rangeKV.getLimit)
      senderRef ! TeleporterEvent.newBuilder(event)
        .setBody(
          KVS.newBuilder()
            .addAllKvs(kvs.map(kv ⇒ KV.newBuilder().setKey(kv.key).setValue(kv.value).build()).asJava)
            .build().toByteString
        ).build()
    case (EventType.KVSave, event) ⇒
      val kv = KV.parseFrom(event.getBody)
      configService.put(key = kv.getKey, value = kv.getValue)
      senderRef ! TeleporterRpc.success(event)
    case (EventType.AtomicSaveKV, event) ⇒
      val atomicSave = AtomicKV.parseFrom(event.getBody)
      configService.atomicPut(atomicSave.getKey, atomicSave.getExpect, atomicSave.getUpdate)
      senderRef ! TeleporterRpc.success(event)
  }

  def defaultEventHandle: EventHandle = {
    case (_, event) ⇒
      if (event.getRole == TeleporterEvent.Role.SERVER) {
        eventListener.resolve(event.getSeqNr, event)
      } else {
        logger.warn(event.toString)
      }
  }

  def reBalance(groupKey: String, currInstance: Option[String] = None): Unit = {
    val group = configService.apply(groupKey).keyBean[GroupValue]
    val tasks = group.value.tasks
    //online instance
    val instances = group.value.instances.filter(runtimeService.get(_).map(_.keyBean[RuntimeInstanceValue])
      .exists(kv ⇒ kv.value.status == InstanceStatus.online))
    //group all partitions
    val partitions = tasks.flatMap(task ⇒ configService.range(Keys.mapping(task, TASK, PARTITIONS)))
      .map(kv ⇒ (kv.key, kv)).toMap
    //current runtime partitions, index by partition key
    val runtimePartitions = tasks.flatMap(task ⇒ runtimeService.range(Keys.mapping(task, TASK, RUNTIME_PARTITIONS)))
      .map(_.keyBean[RuntimePartitionValue])
      .filter(kb ⇒ instances.contains(kb.value.instance))
      .map(kb ⇒ (kb.key, kb)).toMap
    //in used partition and is removed partitions
    val (validRunTimePartitions, invalidRunTimePartitions) = runtimePartitions.partition(kv ⇒ partitions.contains(kv._1))
    //stop already removed partitions
    invalidRunTimePartitions.foreach {
      case (key, kb) ⇒ self ! StopPartition(key, kb.value.instance)
    }
    //exists runtime partitions, group by instance -> instance partitions
    val existsInstPartitions = validRunTimePartitions.values.groupBy(_.value.instance)
    //all instance -> instance partitions, new instance set empty partitions
    val instPartitions = existsInstPartitions ++ (instances -- existsInstPartitions.keys).map(_ → Iterable.empty)
    //compute average partition size
    val avgSize = (partitions.size + instPartitions.keys.size - 1) / instPartitions.keys.size
    val unAssignPartitions = mutable.Queue() ++ (partitions -- validRunTimePartitions.keys).keys
    instPartitions.foreach {
      case (k, kvs) ⇒
        var retainPartitions = kvs
        if (kvs.size > avgSize) {
          val (left, right) = kvs.splitAt(avgSize)
          retainPartitions = left
          right.foreach { kb ⇒
            self ! StopPartition(kb.key, kb.value.instance)
            unAssignPartitions.enqueue(kb.key)
          }
        } else if (kvs.size < avgSize) {
          (1 to (avgSize - kvs.size)).foreach {
            _ ⇒ self ! StartPartition(unAssignPartitions.dequeue(), k)
          }
        }
        //send to current instance again, avoid if broker was kill and instance was reconnection
        currInstance match {
          case Some(curr) if k == curr ⇒
            retainPartitions.foreach(kb ⇒ self ! StartPartition(kb.key, k))
          case None ⇒ retainPartitions.foreach(kb ⇒ self ! StartPartition(kb.key, k))
          case _ ⇒
        }
    }
  }
}

object EventReceiveActor {

  case object Complete

  case class RegisterSender(ref: ActorRef)

  case class StartPartition(partition: String, instance: String)

  case class StopPartition(partition: String, instance: String)

  case class InstanceOffline(instance: String)

  case class ReBalance(group: String, instance: Option[String] = None)

}

object RpcServer extends LazyLogging {
  def apply(host: String, port: Int,
            configService: PersistentService,
            runtimeService: PersistentService,
            connectionKeepers: TrieMap[String, ConnectionKeeper],
            eventListener: EventListener[TeleporterEvent])(implicit mater: ActorMaterializer): Future[Tcp.ServerBinding] = {
    implicit val system = mater.system
    import system.dispatcher
    Tcp().bind(host, port, idleTimeout = 2.minutes).to(Sink.foreach {
      connection ⇒
        logger.info(s"New connection from: ${connection.remoteAddress}")
        val eventReceiver = system.actorOf(Props(classOf[EventReceiveActor], configService, runtimeService, connectionKeepers, eventListener))
        try {
          Source.actorRef[TeleporterEvent](1000, OverflowStrategy.fail)
            .log("server-receive")
            .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
            .map(m ⇒ ByteString(m.toByteArray))
            .watchTermination() { case (ref, fu) ⇒ eventReceiver ! RegisterSender(ref); fu }
            .via(Framing.simpleFramingProtocol(10 * 1024 * 1024).join(connection.flow))
            .filter(_.nonEmpty)
            .map(bs ⇒ TeleporterEvent.parseFrom(bs.toArray))
            .log("server-response")
            .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
            .to(Sink.actorRef(eventReceiver, Complete)).run().onComplete {
            r ⇒ logger.warn(s"Connection was closed, $r")
          }
        } catch {
          case e: Exception ⇒ logger.error(e.getLocalizedMessage, e)
        }
    }).run()
  }
}