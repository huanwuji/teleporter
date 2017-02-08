package teleporter.integration.cluster.broker.tcp

import akka.actor.{Actor, ActorRef, Props, Status}
import akka.event.Logging
import akka.stream.scaladsl.{Framing, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, Attributes, OverflowStrategy}
import akka.util.ByteString
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.broker.ConfigNotify.{Remove, Upsert}
import teleporter.integration.cluster.broker.PersistentProtocol.Keys
import teleporter.integration.cluster.broker.PersistentProtocol.Values._
import teleporter.integration.cluster.broker.PersistentService
import teleporter.integration.cluster.broker.tcp.EventReceiveActor._
import teleporter.integration.cluster.rpc.EventBody.ConfigChangeNotify
import teleporter.integration.cluster.rpc.TeleporterEvent.EventHandle
import teleporter.integration.cluster.rpc._
import teleporter.integration.cluster.rpc.fbs.{Action, EventType, Role}
import teleporter.integration.utils.EventListener

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
                        configNotify: ActorRef,
                        connectionKeepers: TrieMap[String, ConnectionKeeper],
                        eventListener: EventListener[TeleporterEvent[_ <: EventBody]])
  extends Actor with Logging {

  import context.dispatcher
  import teleporter.integration.cluster.broker.PersistentProtocol.Keys._

  var senderRef: ActorRef = _
  var currInstance: String = _

  override def receive: Receive = {
    case Status.Failure(cause) ⇒
      self ! InstanceOffline(this.currInstance)
      logger.error(cause.getMessage, cause)
      connectionKeepers -= this.currInstance
    case Complete ⇒
      self ! InstanceOffline(this.currInstance)
      throw new RuntimeException("Error, Connection can't closed!")
    case RegisterSender(ref) ⇒ senderRef = ref
    case StartPartition(partition, instance) ⇒
      logger.info(s"Assign $partition to $instance")
      runtimeService.put(partition, Version().toJson)
      connectionKeepers.get(instance).foreach { _ ⇒
        eventListener.asyncEvent { seqNr ⇒
          senderRef ! TeleporterEvent(seqNr = seqNr, eventType = EventType.ConfigChangeNotify, role = Role.Request,
            body = ConfigChangeNotify(partition, Action.UPSERT, System.currentTimeMillis()))
        }._2.onComplete {
          case Success(_) ⇒
            runtimeService.put(Keys.mapping(partition, PARTITION, RUNTIME_PARTITION),
              RuntimePartitionValue(instance = instance, status = InstanceStatus.online, timestamp = System.currentTimeMillis()).toJson)
          case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
        }
      }
    case StopPartition(partition, instance) ⇒
      runtimeService.delete(partition)
      connectionKeepers.get(instance).foreach { _ ⇒
        eventListener.asyncEvent { seqNr ⇒
          senderRef ! TeleporterEvent(seqNr = seqNr, eventType = EventType.ConfigChangeNotify, role = Role.Request,
            body = ConfigChangeNotify(partition, Action.REMOVE, System.currentTimeMillis()))
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
    case event: TeleporterEvent[_] if event.role == Role.Request ⇒
      eventHandle.orElse(configEventHandle).apply((event.eventType, event))
    case event: TeleporterEvent[_] if event.role == Role.Response ⇒
      eventListener.resolve(event.seqNr, event)
  }

  def eventHandle: EventHandle = {
    case (EventType.LinkInstance, event) ⇒
      val li = event.toBody[EventBody.LinkInstance]
      val instance = configService(li.instance).keyBean[InstanceValue]
      this.currInstance = instance.key
      connectionKeepers += this.currInstance → ConnectionKeeper(senderRef, self)
      runtimeService.put(
        key = Keys.mapping(li.instance, INSTANCE, RUNTIME_INSTANCE),
        value = RuntimeInstanceValue(
          ip = li.ip,
          port = li.port,
          status = InstanceStatus.online,
          broker = li.broker,
          timestamp = li.timestamp).toJson
      )
      self ! ReBalance(instance.value.group, Some(li.instance))
      senderRef ! TeleporterEvent.success(event)
    case (EventType.LinkAddress, event) ⇒
      val ln = event.toBody[EventBody.LinkAddress]
      runtimeService.put(
        key = Keys(RUNTIME_ADDRESS, Keys.unapply(ln.address, ADDRESS) ++ Keys.unapply(ln.instance, INSTANCE)),
        value = RuntimeAddressValue(keys = ln.keys.toSet, timestamp = ln.timestamp).toJson
      )
      senderRef ! TeleporterEvent.success(event)
    case (EventType.LinkVariable, event) ⇒
      val ln = event.toBody[EventBody.LinkVariable]
      runtimeService.put(
        key = Keys(RUNTIME_VARIABLE, Keys.unapply(ln.variableKey, VARIABLE) ++ Keys.unapply(ln.instance, INSTANCE)),
        value = RuntimeVariableValue(ln.keys.toSet, timestamp = ln.timestamp).toJson
      )
      senderRef ! TeleporterEvent.success(event)
    case (EventType.LogTail, event) ⇒
      eventListener.resolve(event.seqNr, event)
  }

  def configEventHandle: EventHandle = {
    case (EventType.KVGet, event) ⇒
      val get = event.toBody[EventBody.KVGet]
      val kv = configService(get.key)
      senderRef ! event.copy(role = Role.Response, body = EventBody.KV(kv.key, kv.value))
    case (EventType.RangeRegexKV, event) ⇒
      val rangeKV = event.toBody[EventBody.RangeRegexKV]
      val kvs = configService.regexRange(rangeKV.key, rangeKV.start, rangeKV.limit)
      senderRef ! event.copy(role = Role.Response, body = EventBody.KVS(kvs.map(kv ⇒ EventBody.KV(kv.key, kv.value))))
    case (EventType.KVSave, event) ⇒
      val kv = event.toBody[EventBody.KV]
      configService.put(key = kv.key, value = kv.value)
      senderRef ! TeleporterEvent.success(event)
    case (EventType.KVRemove, event) ⇒
      val remove = event.toBody[EventBody.KVRemove]
      configService.delete(key = remove.key)
      senderRef ! TeleporterEvent.success(event)
    case (EventType.AtomicSaveKV, event) ⇒
      val atomicSave = event.toBody[EventBody.AtomicKV]
      if (configService.atomicPut(atomicSave.key, atomicSave.expect, atomicSave.update)) {
        senderRef ! TeleporterEvent.success(event)
      } else {
        senderRef ! TeleporterEvent.failure(event)
      }
    case (EventType.ConfigChangeNotify, event) ⇒
      val notify = event.toBody[ConfigChangeNotify]
      notify.action match {
        case Action.ADD | Action.UPDATE | Action.UPSERT ⇒ Upsert(notify.key)
        case Action.REMOVE ⇒ configNotify ! Remove(notify.key)
      }
      senderRef ! TeleporterEvent.success(event)
  }

  def reBalance(groupKey: String, currInstance: Option[String] = None): Unit = {
    val groupOpt = configService.get(groupKey).map(_.keyBean[GroupValue])
    require(groupOpt.isDefined, s"Please config group: $groupKey in the ui")
    val group = groupOpt.get
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

object RpcServer extends Logging {
  def apply(host: String, port: Int,
            configService: PersistentService,
            runtimeService: PersistentService,
            configNotify: ActorRef,
            connectionKeepers: TrieMap[String, ConnectionKeeper],
            eventListener: EventListener[TeleporterEvent[_ <: EventBody]])(implicit mater: ActorMaterializer): Future[Tcp.ServerBinding] = {
    implicit val system = mater.system
    import system.dispatcher
    Tcp().bind(host, port, idleTimeout = 2.minutes).to(Sink.foreach {
      connection ⇒
        logger.info(s"New connection from: ${connection.remoteAddress}")
        val eventReceiver = system.actorOf(Props(classOf[EventReceiveActor], configService, runtimeService, configNotify, connectionKeepers, eventListener))
        try {
          Source.actorRef[TeleporterEvent[_ <: EventBody]](1000, OverflowStrategy.fail)
            .log("server-send")
            .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
            .map(event ⇒ ByteString(TeleporterEvent.toArray(event)))
            .watchTermination() { case (ref, fu) ⇒ eventReceiver ! RegisterSender(ref); fu }
            .via(Framing.simpleFramingProtocol(10 * 1024 * 1024).join(connection.flow))
            .filter(_.nonEmpty)
            .map(TeleporterEvent(_))
            .log("server-receiver")
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