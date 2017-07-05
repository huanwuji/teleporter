package teleporter.integration.cluster.broker.tcp

import akka.actor.{Actor, ActorRef, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Tcp}
import com.typesafe.config.Config
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.broker.ConfigNotify.{Remove, Upsert}
import teleporter.integration.cluster.broker.PersistentProtocol.Keys
import teleporter.integration.cluster.broker.PersistentProtocol.Keys._
import teleporter.integration.cluster.broker.PersistentProtocol.Values._
import teleporter.integration.cluster.broker.PersistentService
import teleporter.integration.cluster.broker.tcp.InstanceManager.{InstanceOffline, ReBalance, StartPartition, StopPartition}
import teleporter.integration.cluster.rpc.EventBody.ConfigChangeNotify
import teleporter.integration.cluster.rpc._
import teleporter.integration.cluster.rpc.fbs.{Action, MessageType, Role}
import teleporter.integration.utils.EventListener

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by kui.dai on 2016/7/26.
  */
object InstanceManager {

  case object Complete

  case class RegisterSender(ref: ActorRef)

  case class StartPartition(partition: String, instance: String)

  case class StopPartition(partition: String, instance: String)

  case class InstanceOffline(instance: String)

  case class ReBalance(group: String, instance: Option[String] = None)

}

class InstanceManager(configService: PersistentService,
                      runtimeService: PersistentService,
                      connectionKeepers: TrieMap[String, RpcServerConnectionHandler]
                     )(implicit eventListener: EventListener[fbs.RpcMessage]) extends Actor with Logging {

  import context.dispatcher

  override def receive: Receive = {
    case StartPartition(partition, instance) ⇒
      logger.info(s"Assign $partition to $instance")
      runtimeService.put(partition, Version().toJson)
      connectionKeepers.get(instance).foreach { handler ⇒
        handler.request(MessageType.ConfigChangeNotify, ConfigChangeNotify(partition, Action.UPSERT, System.currentTimeMillis()).toArray)
          .onComplete {
            case Success(_) ⇒
              runtimeService.put(Keys.mapping(partition, PARTITION, RUNTIME_PARTITION),
                RuntimePartitionValue(instance = instance, status = InstanceStatus.online, timestamp = System.currentTimeMillis()).toJson)
            case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
          }
      }
    case StopPartition(partition, instance) ⇒ runtimeService.delete(partition)
      connectionKeepers.get(instance).foreach { handler ⇒
        handler.request(MessageType.ConfigChangeNotify, ConfigChangeNotify(partition, Action.REMOVE, System.currentTimeMillis()).toArray)
          .onComplete {
            case Success(_) ⇒ runtimeService.delete(Keys.mapping(partition, PARTITION, RUNTIME_PARTITION))
            case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
          }
      }
    case InstanceOffline(instance) ⇒
      logger.info(s"Instance:$instance was offline")
      connectionKeepers -= instance
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

object RpcServer extends Logging {
  def apply(config: Config,
            configService: PersistentService,
            runtimeService: PersistentService,
            configNotify: ActorRef,
            connectionKeepers: TrieMap[String, RpcServerConnectionHandler],
            eventListener: EventListener[fbs.RpcMessage])(implicit mater: ActorMaterializer): Future[Tcp.ServerBinding] = {
    implicit val system = mater.system
    import system.dispatcher
    val instanceManager = system.actorOf(Props(classOf[InstanceManager], configService, runtimeService, connectionKeepers, eventListener))
    Tcp().bind(interface = config.getString("bind"), port = config.getInt("port"), idleTimeout = 2.minutes).to(Sink.foreach {
      connection ⇒
        logger.info(s"New connection from: ${connection.remoteAddress}")
        var instanceKey: String = null
        var handler: RpcServerConnectionHandler = null
        handler = RpcServerConnectionHandler(connection, message ⇒ {
          message.role() match {
            case Role.Request ⇒
              message.messageType() match {
                case MessageType.LinkInstance ⇒
                  val li = RpcRequest.decode(message, EventBody.LinkInstance(_)).body.get
                  val instance = configService(li.instance).keyBean[InstanceValue]
                  instanceKey = instance.key
                  connectionKeepers += (instanceKey → handler)
                  runtimeService.put(
                    key = Keys.mapping(li.instance, INSTANCE, RUNTIME_INSTANCE),
                    value = RuntimeInstanceValue(
                      ip = li.ip,
                      port = li.port,
                      status = InstanceStatus.online,
                      broker = li.broker,
                      timestamp = li.timestamp).toJson
                  )
                  instanceManager ! ReBalance(instance.value.group, Some(li.instance))
                  handler.response(RpcResponse.success(message))
                case MessageType.LinkAddress ⇒
                  val ln = RpcRequest.decode(message, EventBody.LinkAddress(_)).body.get
                  runtimeService.put(
                    key = Keys(RUNTIME_ADDRESS, Keys.unapply(ln.address, ADDRESS) ++ Keys.unapply(ln.instance, INSTANCE)),
                    value = RuntimeAddressValue(keys = ln.keys.toSet, timestamp = ln.timestamp).toJson
                  )
                  handler.response(RpcResponse.success(message))
                case MessageType.LinkVariable ⇒
                  val ln = RpcRequest.decode(message, EventBody.LinkVariable(_)).body.get
                  runtimeService.put(
                    key = Keys(RUNTIME_VARIABLE, Keys.unapply(ln.variableKey, VARIABLE) ++ Keys.unapply(ln.instance, INSTANCE)),
                    value = RuntimeVariableValue(ln.keys.toSet, timestamp = ln.timestamp).toJson
                  )
                  handler.response(RpcResponse.success(message))
                case MessageType.LogTail ⇒
                  eventListener.resolve(message.seqNr, message)

                case MessageType.KVGet ⇒
                  val get = RpcRequest.decode(message, EventBody.KVGet(_)).body.get
                  val kv = configService(get.key)
                  handler.response(RpcResponse.success(message, EventBody.KV(kv.key, kv.value).toArray))
                case MessageType.RangeRegexKV ⇒
                  val rangeKV = RpcRequest.decode(message, EventBody.RangeRegexKV(_)).body.get
                  val kvs = configService.regexRange(rangeKV.key, rangeKV.start, rangeKV.limit)
                  handler.response(RpcResponse.success(message, EventBody.KVS(kvs.map(kv ⇒ EventBody.KV(kv.key, kv.value))).toArray))
                case MessageType.KVSave ⇒
                  val kv = RpcRequest.decode(message, EventBody.KV(_)).body.get
                  configService.put(key = kv.key, value = kv.value)
                  handler.response(RpcResponse.success(message))
                case MessageType.KVRemove ⇒
                  val remove = RpcRequest.decode(message, EventBody.KVRemove(_)).body.get
                  configService.delete(key = remove.key)
                  handler.response(RpcResponse.success(message))
                case MessageType.AtomicSaveKV ⇒
                  val atomicSave = RpcRequest.decode(message, EventBody.AtomicKV(_)).body.get
                  if (configService.atomicPut(atomicSave.key, atomicSave.expect, atomicSave.update)) {
                    handler.response(RpcResponse.success(message, EventBody.KV(atomicSave.key, atomicSave.update).toArray))
                  } else {
                    handler.response(RpcResponse.failure(message, s"Atomic key:${atomicSave.key} save conflict!"))
                  }
                case MessageType.ConfigChangeNotify ⇒
                  val notify = RpcRequest.decode(message, EventBody.ConfigChangeNotify(_)).body.get
                  notify.action match {
                    case Action.ADD | Action.UPDATE | Action.UPSERT ⇒ Upsert(notify.key)
                    case Action.REMOVE ⇒ configNotify ! Remove(notify.key)
                  }
                  handler.response(RpcResponse.success(message))
              }
            case Role.Response ⇒ eventListener.resolve(message.seqNr(), message)
          }
        })
        handler.sourceQueue.watchCompletion().onComplete {
          case Success(_) ⇒ instanceManager ! InstanceOffline(instanceKey)
          case Failure(ex) ⇒
            logger.error(ex.getLocalizedMessage, ex)
            instanceManager ! InstanceOffline(instanceKey)
        }
    }).run()
  }
}