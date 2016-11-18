package teleporter.integration.cluster.broker

import akka.actor.Actor
import teleporter.integration.cluster.broker.ConfigNotify.{Remove, Upsert}
import teleporter.integration.cluster.broker.PersistentProtocol.Keys._
import teleporter.integration.cluster.broker.PersistentProtocol.Values.{GroupValue, PartitionValue, RuntimePartitionValue, TaskValue}
import teleporter.integration.cluster.broker.PersistentProtocol.{Keys, Tables}
import teleporter.integration.cluster.broker.tcp.ConnectionKeeper
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.instance.Instance.ConfigChangeNotify

import scala.collection.concurrent.TrieMap

/**
  * @author kui.dai Created 2016/8/30
  */
object ConfigNotify {

  case class Upsert(key: String)

  case class Remove(key: String)

}

class ConfigNotify(connectionKeepers: TrieMap[String, ConnectionKeeper], configService: PersistentService, runtimeService: PersistentService) extends Actor {
  override def receive: Receive = {
    case Upsert(key) ⇒
      Keys.table(key) match {
        case Tables.task ⇒ notifyTask(key, ConfigChangeNotify.Action.UPSERT)
        case Tables.group ⇒ notifyGroup(key, ConfigChangeNotify.Action.UPDATE)
        case Tables.partition ⇒ notifyPartition(key, ConfigChangeNotify.Action.UPDATE)
        case Tables.stream ⇒ notifyStream(key, key, ConfigChangeNotify.Action.UPDATE)
        case Tables.source ⇒ notifySource(key, key, ConfigChangeNotify.Action.UPDATE)
        case Tables.sink ⇒ notifySink(key, key, ConfigChangeNotify.Action.UPDATE)
        case Tables.address ⇒ notifyAddress(key, key, ConfigChangeNotify.Action.UPDATE)
        case Tables.variable ⇒ notifyVariable(key, key, ConfigChangeNotify.Action.UPDATE)
      }
    case Remove(key) ⇒
      Keys.table(key) match {
        case Tables.task ⇒ notifyTask(key, ConfigChangeNotify.Action.REMOVE)
        case Tables.group ⇒ notifyGroup(key, ConfigChangeNotify.Action.REMOVE)
        case Tables.partition ⇒ notifyPartition(key, ConfigChangeNotify.Action.REMOVE)
        case Tables.stream ⇒ notifyStream(key, key, ConfigChangeNotify.Action.REMOVE)
        case Tables.source ⇒ notifySource(key, key, ConfigChangeNotify.Action.REMOVE)
        case Tables.sink ⇒ notifySink(key, key, ConfigChangeNotify.Action.REMOVE)
        case Tables.address ⇒ notifyAddress(key, key, ConfigChangeNotify.Action.REMOVE)
        case Tables.variable ⇒ notifyVariable(key, key, ConfigChangeNotify.Action.REMOVE)
      }
  }

  def notifyTask(key: String, action: ConfigChangeNotify.Action): Unit = {
    configService.get(key).map(_.keyBean[TaskValue]).foreach { task ⇒
      configService.get(task.value.group).map(_.keyBean[GroupValue]).foreach { group ⇒
        group.value.instances.foreach { instanceKey ⇒
          connectionKeepers.get(instanceKey).foreach(_.senderRef ! notifyEvent(key, action))
        }
      }
    }
  }

  def notifyGroup(key: String, action: ConfigChangeNotify.Action): Unit = {
    configService.get(key).map(_.keyBean[GroupValue]).foreach { group ⇒
      group.value.instances.foreach { instanceKey ⇒
        connectionKeepers.get(instanceKey).foreach(_.senderRef ! notifyEvent(key, action))
      }
    }
  }

  def notifyPartition(key: String, action: ConfigChangeNotify.Action): Unit = {
    runtimeService.get(Keys.mapping(key, PARTITION, RUNTIME_PARTITION)).map(_.keyBean[RuntimePartitionValue]).foreach { runtimePartition ⇒
      connectionKeepers.get(runtimePartition.value.instance).foreach(_.senderRef ! notifyEvent(key, action))
    }
  }

  def notifyStream(key: String, notifyKey: String, action: ConfigChangeNotify.Action): Unit = {
    configService.range(Keys.mapping(key, STREAM, PARTITIONS)).map(_.keyBean[PartitionValue]).foreach { partition ⇒
      if (partition.value.keys.exists(Keys.belongRegex(_, key))) {
        runtimeService.get(partition.key).map(_.keyBean[RuntimePartitionValue]).foreach { runtimePartition ⇒
          connectionKeepers.get(runtimePartition.value.instance).foreach(_.senderRef ! notifyEvent(notifyKey, action))
        }
      }
    }
  }

  def notifySource(key: String, notifyKey: String, action: ConfigChangeNotify.Action): Unit = {
    notifyStream(Keys.mapping(key, SOURCE, STREAM), key, action)
  }

  def notifySink(key: String, notifyKey: String, action: ConfigChangeNotify.Action): Unit = {
    notifyStream(Keys.mapping(key, SINK, STREAM), key, action)
  }

  def notifyAddress(key: String, notifyKey: String, action: ConfigChangeNotify.Action): Unit = {
    runtimeService.range(Keys.mapping(key, ADDRESS, RUNTIME_ADDRESSES)).foreach { runtimeAddress ⇒
      connectionKeepers.get(Keys.mapping(runtimeAddress.key, RUNTIME_ADDRESS, INSTANCE))
        .foreach(_.senderRef ! notifyEvent(notifyKey, action))
    }
  }

  def notifyVariable(key: String, notifyKey: String, action: ConfigChangeNotify.Action): Unit = {
    runtimeService.range(Keys.mapping(key, VARIABLE, RUNTIME_VARIABLE)).foreach { runtimeVariable ⇒
      connectionKeepers.get(Keys.mapping(runtimeVariable.key, RUNTIME_VARIABLE, INSTANCE))
        .foreach(_.senderRef ! notifyEvent(notifyKey, action))
    }
  }

  def notifyEvent(key: String, action: ConfigChangeNotify.Action): TeleporterEvent = TeleporterEvent.newBuilder()
    //                    .setSeqNr(seqNr)
    .setRole(TeleporterEvent.Role.SERVER)
    .setType(EventType.ConfigChangeNotify)
    .setBody(
      ConfigChangeNotify.newBuilder().setKey(key).setAction(action).setTimestamp(System.currentTimeMillis()).build().toByteString
    ).build()
}