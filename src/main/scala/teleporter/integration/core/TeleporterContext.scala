package teleporter.integration.core

import akka.actor.{Actor, ActorRef, Props}
import teleporter.integration.ClientApply
import teleporter.integration.cluster.broker.PersistentProtocol.Keys
import teleporter.integration.cluster.broker.PersistentProtocol.Keys._
import teleporter.integration.cluster.broker.PersistentProtocol.Values.PartitionValue
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.broker.Broker.{LinkAddress, LinkVariable}
import teleporter.integration.core.TeleporterConfig._
import teleporter.integration.core.TeleporterContext.{SyncBroker, _}
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{MultiIndexMap, TwoIndexMap}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable


/**
  * Author: kui.dai
  * Date: 2016/6/27.
  */
trait ComponentContext {
  val id: Long
  val key: String
}

case class PartitionContext(id: Long, key: String, config: PartitionValue) extends ComponentContext {
  def streams()(implicit center: TeleporterCenter): Map[String, StreamContext] = {
    config.keys.flatMap(center.context.indexes.regexRangeTo[StreamContext]).toMap
  }
}

case class TaskContext(id: Long, key: String, config: TaskConfig,
                       variableKeys: Set[String], streamSchedule: ActorRef = ActorRef.noSender) extends ComponentContext {
  def streams()(implicit center: TeleporterCenter): mutable.Map[String, StreamContext] = {
    center.context.indexes.rangeTo[StreamContext](Keys(STREAMS, Keys.unapply(key, TASK)))
  }

  def sources()(implicit center: TeleporterCenter): mutable.Map[String, SourceContext] = {
    center.context.indexes.rangeTo[SourceContext](Keys(TASK_SOURCES, Keys.unapply(key, TASK)))
  }

  def sinks()(implicit center: TeleporterCenter): mutable.Map[String, SinkContext] = {
    center.context.indexes.rangeTo[SinkContext](Keys(TASK_SOURCES, Keys.unapply(key, SINK)))
  }
}

case class StreamContext(id: Long, key: String, config: StreamConfig, variableKeys: Set[String]) extends ComponentContext {
  def task()(implicit center: TeleporterCenter): TaskContext = {
    center.context.getContext[TaskContext](Keys(TASK, Keys.unapply(key, STREAM)))
  }

  def sources()(implicit center: TeleporterCenter): mutable.Map[String, SourceContext] = {
    center.context.indexes.rangeTo[SourceContext](Keys(STREAM_SOURCES, Keys.unapply(key, TASK)))
  }

  def sinks()(implicit center: TeleporterCenter): mutable.Map[String, SinkContext] = {
    center.context.indexes.rangeTo[SinkContext](Keys(STREAM_SINKS, Keys.unapply(key, SINK)))
  }
}

case class SourceContext(id: Long, key: String, config: SourceConfig,
                         variableKeys: Set[String], var actorRef: ActorRef = ActorRef.noSender) extends ComponentContext {
  def task()(implicit center: TeleporterCenter): TaskContext = {
    center.context.getContext[TaskContext](Keys(TASK, Keys.unapply(key, SOURCE)))
  }

  def stream()(implicit center: TeleporterCenter): StreamContext = {
    center.context.getContext[StreamContext](Keys(STREAM, Keys.unapply(key, SOURCE)))
  }

  def addressKey = config[String]("address")

  def address()(implicit center: TeleporterCenter): AddressContext = {
    center.context.getContext[AddressContext](config[String]("address"))
  }
}

case class SinkContext(id: Long, key: String, config: SinkConfig,
                       variableKeys: Set[String], var actorRef: ActorRef = ActorRef.noSender) extends ComponentContext {
  def task()(implicit center: TeleporterCenter): TaskContext = {
    center.context.getContext[TaskContext](Keys(TASK, Keys.unapply(key, SOURCE)))
  }

  def stream()(implicit center: TeleporterCenter): StreamContext = {
    center.context.getContext[StreamContext](Keys(STREAM, Keys.unapply(key, SINK)))
  }

  def addressKey = config[String]("address")

  def address()(implicit center: TeleporterCenter): AddressContext = {
    center.context.getContext[AddressContext](config[String]("address"))
  }
}

object ClientRefs {
  def apply[A](share: Boolean): ClientRefs[A] =
    share match {
      case true ⇒ new ShareClientRefs[A]
      case false ⇒ new MultiClientRefs[A]
    }
}

trait ClientRefs[A] {
  def apply(key: String, clientApply: ClientApply[A])(implicit center: TeleporterCenter): A

  def close(key: String)(implicit center: TeleporterCenter): Unit
}

class ShareClientRefs[A] extends ClientRefs[A] {
  var clientRef: ClientRef[A] = _
  var keys: Set[String] = Set.empty

  override def apply(key: String, clientApply: ClientApply[A])(implicit center: TeleporterCenter): A = {
    synchronized {
      if (clientRef == null) {
        clientRef = clientApply(key, center)
        keys = keys + key
      }
    }
    clientRef.client
  }

  override def close(key: String)(implicit center: TeleporterCenter): Unit = {
    synchronized {
      keys = keys - key
      if (keys.isEmpty) {
        clientRef match {
          case client: AutoCloseClientRef[A] ⇒ client.close()
          case _ ⇒ //Nothing to do
        }
      }
    }
  }
}

class MultiClientRefs[A] extends ClientRefs[A] {
  val clientRefs = TrieMap[String, ClientRef[A]]()

  override def apply(key: String, clientApply: ClientApply[A])(implicit center: TeleporterCenter): A = {
    clientRefs.getOrElseUpdate(key, clientApply(key, center)).client
  }

  override def close(key: String)(implicit center: TeleporterCenter): Unit = {
    clientRefs.remove(key).foreach {
      case client: AutoCloseClientRef[A] ⇒ client.close()
      case _ ⇒ //Nothing to do
    }
  }
}

case class AddressContext(id: Long, key: String, var config: AddressConfig,
                          linkKeys: Set[String], variableKeys: Set[String], clientRefs: ClientRefs[Any]) extends ComponentContext

case class VariableContext(id: Long, key: String, var config: VariableConfig, linkKeys: Set[String]) extends ComponentContext

trait TeleporterContext {
  val indexes: TwoIndexMap[Long, ComponentContext]

  val ref: ActorRef

  def getContext[A <: ComponentContext](id: Long): A = indexes.applyKey1(id).asInstanceOf[A]

  def getContext[A <: ComponentContext](name: String): A = indexes.applyKey2(name).asInstanceOf[A]
}

class TeleporterContextImpl(val ref: ActorRef, val indexes: TwoIndexMap[Long, ComponentContext]) extends TeleporterContext

class TeleporterContextActor(indexes: TwoIndexMap[Long, ComponentContext])(implicit center: TeleporterCenter) extends Actor {
  override def receive: Receive = updateContext().orElse(triggerChange)

  private def updateContext(): Receive = {
    case Upsert(ctx: ComponentContext, trigger) ⇒
      if (indexes.getKey1(ctx.id).isDefined) {
        self ! Update(ctx, trigger)
      } else {
        self ! Add(ctx, trigger)
      }
    case Add(ctx: ComponentContext, trigger) ⇒
      indexes += (ctx.id, ctx.key, ctx)
      ctx match {
        case ctx: PartitionContext ⇒
        case ctx: TaskContext ⇒
        case ctx: StreamContext ⇒
        case ctx: SourceContext ⇒
          indexes.modifyByKey2(ctx.addressKey, { case addressCtx: AddressContext ⇒
            addressCtx.copy(linkKeys = addressCtx.linkKeys + ctx.key)
          })
          self ! SyncBroker(indexes.applyKey2(ctx.addressKey))
        case ctx: SinkContext ⇒
          indexes.modifyByKey2(ctx.addressKey, {
            case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys + ctx.key)
          })
          self ! SyncBroker(indexes.applyKey2(ctx.addressKey))
        case ctx: AddressContext ⇒
        case ctx: VariableContext ⇒
      }
      if (trigger) self ! TriggerAdd(ctx)
    case Update(ctx: ComponentContext, trigger) ⇒
      ctx match {
        case ctx: PartitionContext ⇒
        case ctx: TaskContext ⇒
        case ctx: StreamContext ⇒
        case ctx: SourceContext ⇒
          val oldSourceContext = center.context.getContext[SourceContext](ctx.id)
          if (oldSourceContext.addressKey != ctx.addressKey) {
            indexes.modifyByKey2(oldSourceContext.addressKey, {
              case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys - ctx.key)
            })
            indexes.modifyByKey2(ctx.addressKey, {
              case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys + ctx.key)
            })
          }
          self ! SyncBroker(indexes.applyKey2(ctx.addressKey))
        case ctx: SinkContext ⇒
          val oldSinkContext = center.context.getContext[SinkContext](ctx.id)
          if (oldSinkContext.addressKey != ctx.addressKey) {
            indexes.modifyByKey2(oldSinkContext.addressKey, {
              case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys - ctx.key)
            })
            indexes.modifyByKey2(ctx.addressKey, {
              case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys + ctx.key)
            })
          }
          self ! SyncBroker(indexes.applyKey2(ctx.addressKey))
        case ctx: AddressContext ⇒
        case ctx: VariableContext ⇒
      }
      indexes += (ctx.id, ctx.key, ctx)
      if (trigger) self ! TriggerUpdate(ctx)
    case Remove(ctx: ComponentContext, trigger) ⇒
      indexes.removeKey1(ctx.id)
      ctx match {
        case ctx: PartitionContext ⇒ ctx.streams().foreach(self ! Remove(_))
        case ctx: TaskContext ⇒ ctx.streams().foreach(self ! Remove(_))
        case ctx: StreamContext ⇒
          ctx.sources().foreach(self ! Remove(_))
          ctx.sinks().foreach(self ! Remove(_))
        case ctx: SourceContext ⇒
          indexes.modifyByKey2(ctx.addressKey, {
            case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys - ctx.key)
          })
          self ! SyncBroker(indexes.applyKey2(ctx.addressKey))
        case ctx: SinkContext ⇒
          indexes.modifyByKey2(ctx.addressKey, {
            case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys - ctx.key)
          })
          self ! SyncBroker(indexes.applyKey2(ctx.addressKey))
        case ctx: AddressContext ⇒
        case ctx: VariableContext ⇒
      }
      if (trigger) self ! TriggerRemove(ctx)
    case SyncBroker(ctx) ⇒
      ctx match {
        case ctx: AddressContext ⇒
          center.eventListener.asyncEvent { seqNr ⇒
            center.brokers ! SendMessage(TeleporterEvent.newBuilder()
              .setRole(TeleporterEvent.Role.CLIENT)
              .setSeqNr(seqNr)
              .setType(EventType.LinkAddress)
              .setBody(
                LinkAddress.newBuilder()
                  .setAddress(ctx.key)
                  .setInstance(center.instance)
                  .addAllKeys(ctx.linkKeys.asJava)
                  .setTimestamp(System.currentTimeMillis())
                  .build().toByteString
              )
              .build())
          }
        case ctx: VariableContext ⇒
          center.eventListener.asyncEvent { seqNr ⇒
            center.brokers ! SendMessage(TeleporterEvent.newBuilder()
              .setRole(TeleporterEvent.Role.CLIENT)
              .setSeqNr(seqNr)
              .setType(EventType.LinkVariable)
              .setBody(
                LinkVariable.newBuilder()
                  .setVariableKey(ctx.key)
                  .setInstance(center.instance)
                  .addAllKeys(ctx.linkKeys.asJava)
                  .build().toByteString
              )
              .build())
          }
      }
  }

  private def triggerChange: Receive = {
    case TriggerUpsert(ctx: ComponentContext) ⇒
      if (indexes.getKey1(ctx.id).isDefined) {
        self ! TriggerUpdate(ctx)
      } else {
        self ! TriggerAdd(ctx)
      }
    case TriggerAdd(ctx) ⇒
      ctx match {
        case ctx: PartitionContext ⇒
          ctx.streams().foreach(t2 ⇒ center.streams ! Streams.Start(t2._1))
        case ctx: TaskContext ⇒
          ctx.streams().foreach(t2 ⇒ center.streams ! Streams.Start(t2._1))
        case ctx: StreamContext ⇒
          center.streams ! Streams.Start(ctx.key)
        case ctx: SourceContext ⇒
          center.streams ! Streams.Start(Keys.mapping(ctx.key, SOURCE, STREAM))
        case ctx: SinkContext ⇒
          center.streams ! Streams.Start(Keys.mapping(ctx.key, SINK, STREAM))
        case ctx: AddressContext ⇒
          ctx.linkKeys.map(Keys.mapping(_, STREAM, STREAM)).foreach(center.streams ! Streams.Start(_))
        case ctx: VariableContext ⇒
          ctx.linkKeys.map(Keys.mapping(_, STREAM, STREAM)).foreach(center.streams ! Streams.Start(_))
      }
    case TriggerUpdate(ctx) ⇒
      ctx match {
        case ctx: PartitionContext ⇒
          ctx.streams().foreach(t2 ⇒ center.streams ! Streams.Restart(t2._1))
        case ctx: TaskContext ⇒
          ctx.streams().foreach(t2 ⇒ center.streams ! Streams.Restart(t2._1))
        case ctx: StreamContext ⇒
          center.streams ! Streams.Restart(ctx.key)
        case ctx: SourceContext ⇒
          center.streams ! Streams.Restart(Keys.mapping(ctx.key, SOURCE, STREAM))
        case ctx: SinkContext ⇒
          center.streams ! Streams.Restart(Keys.mapping(ctx.key, SINK, STREAM))
        case ctx: AddressContext ⇒
          ctx.linkKeys.map(Keys.mapping(_, STREAM, STREAM)).foreach(center.streams ! Streams.Restart(_))
        case ctx: VariableContext ⇒
          ctx.linkKeys.map(Keys.mapping(_, STREAM, STREAM)).foreach(center.streams ! Streams.Restart(_))
      }
    case TriggerRemove(ctx) ⇒
      ctx match {
        case ctx: PartitionContext ⇒
          ctx.streams().foreach(t2 ⇒ center.streams ! Streams.Remove(t2._1))
        case ctx: TaskContext ⇒
          ctx.streams().foreach(t2 ⇒ center.streams ! Streams.Remove(t2._1))
        case ctx: StreamContext ⇒
          center.streams ! Streams.Remove(ctx.key)
        case ctx: SourceContext ⇒
          center.streams ! Streams.Remove(Keys.mapping(ctx.key, SOURCE, STREAM))
        case ctx: SinkContext ⇒
          center.streams ! Streams.Remove(Keys.mapping(ctx.key, SINK, STREAM))
        case ctx: AddressContext ⇒
          ctx.linkKeys.map(Keys.mapping(_, STREAM, STREAM)).foreach(center.streams ! Streams.Remove(_))
        case ctx: VariableContext ⇒
          ctx.linkKeys.map(Keys.mapping(_, STREAM, STREAM)).foreach(center.streams ! Streams.Remove(_))
      }
  }
}

object TeleporterContext {

  sealed trait Action

  sealed trait TriggerChange

  case class Add[T >: ComponentContext](ctx: T, trigger: Boolean = false) extends Action

  case class Update[T >: ComponentContext](ctx: T, trigger: Boolean = false) extends Action

  case class Upsert[T >: ComponentContext](ctx: T, trigger: Boolean = false) extends Action

  case class Remove[T >: ComponentContext](ctx: T, trigger: Boolean = false) extends Action

  case class TriggerAdd[T >: ComponentContext](ctx: T) extends TriggerChange

  case class TriggerUpdate[T >: ComponentContext](ctx: T) extends TriggerChange

  case class TriggerUpsert[T >: ComponentContext](ctx: T) extends TriggerChange

  case class TriggerRemove[T >: ComponentContext](ctx: T) extends TriggerChange

  case class SyncBroker[T >: ComponentContext](ctx: T)

  def apply()(implicit center: TeleporterCenter): TeleporterContext = {
    val indexes = MultiIndexMap[Long, ComponentContext]()
    val ref = center.system.actorOf(Props(classOf[TeleporterContextActor], indexes, center))
    new TeleporterContextImpl(ref, indexes)
  }
}