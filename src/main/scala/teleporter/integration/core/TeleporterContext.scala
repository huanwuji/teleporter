package teleporter.integration.core

import akka.actor.{Actor, ActorRef, Props}
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.ActorTestMessages.{Ping, Pong}
import teleporter.integration.cluster.broker.PersistentProtocol.Keys._
import teleporter.integration.cluster.broker.PersistentProtocol.{Keys, Tables}
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.fbs.generate.{EventType, Role}
import teleporter.integration.cluster.rpc.{LinkAddress, LinkVariable, TeleporterEvent}
import teleporter.integration.core.TeleporterContext.{SyncBroker, _}
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{MultiIndexMap, TwoIndexMap}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable


/**
  * Author: kui.dai
  * Date: 2016/6/27.
  */
trait ComponentContext {
  val id: Long
  val key: String
  val config: ConfigMetaBean
}

trait ExtraKeys {
  self: ComponentContext ⇒
  def extraKeys[T <: ComponentContext](innerKey: String)(implicit center: TeleporterCenter): T = {
    val key = self.config.extraKeys[String](innerKey)
    center.context.getContext[T](key)
  }
}

case class PartitionContext(id: Long, key: String, config: PartitionMetaBean) extends ExtraKeys with ComponentContext {
  def streams()(implicit center: TeleporterCenter): Map[String, StreamContext] = {
    config.keys.flatMap(center.context.indexes.regexRangeTo[StreamContext]).toMap
  }
}

case class TaskContext(id: Long, key: String, config: TaskMetaBean, streamSchedule: ActorRef = ActorRef.noSender) extends ExtraKeys with ComponentContext {
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

case class StreamContext(id: Long, key: String, config: StreamMetaBean) extends ExtraKeys with ComponentContext {
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

case class SourceContext(id: Long, key: String, config: SourceMetaBean, actorRef: ActorRef = ActorRef.noSender) extends ExtraKeys with ComponentContext {
  def task()(implicit center: TeleporterCenter): TaskContext = {
    center.context.getContext[TaskContext](Keys(TASK, Keys.unapply(key, SOURCE)))
  }

  def stream()(implicit center: TeleporterCenter): StreamContext = {
    center.context.getContext[StreamContext](Keys(STREAM, Keys.unapply(key, SOURCE)))
  }

  def address()(implicit center: TeleporterCenter): AddressContext = {
    center.context.getContext[AddressContext](config.address)
  }
}

case class SinkContext(id: Long, key: String, config: SinkMetaBean, actorRef: ActorRef = ActorRef.noSender) extends ExtraKeys with ComponentContext {
  def task()(implicit center: TeleporterCenter): TaskContext = {
    center.context.getContext[TaskContext](Keys(TASK, Keys.unapply(key, SOURCE)))
  }

  def stream()(implicit center: TeleporterCenter): StreamContext = {
    center.context.getContext[StreamContext](Keys(STREAM, Keys.unapply(key, SINK)))
  }

  def address()(implicit center: TeleporterCenter): AddressContext = {
    center.context.getContext[AddressContext](config.address)
  }
}

trait ClientRef[+A] {
  val key: String
  val client: A
}

abstract class CloseClientRef[A](val key: String, val client: A) extends ClientRef[A] with AutoCloseable {
  override def close(): Unit
}

case class AutoCloseClientRef[A <: AutoCloseable](override val key: String, override val client: A) extends CloseClientRef[A](key, client) {
  override def close(): Unit = client.close()
}

case class AddressContext(id: Long, key: String, config: AddressMetaBean, linkKeys: Set[String]) extends ExtraKeys with ComponentContext

case class VariableContext(id: Long, key: String, config: VariableMetaBean, linkKeys: Set[String]) extends ComponentContext

trait Addresses extends Logging {
  private val addressIndexes = new TrieMap[String, TrieMap[String, CloseClientRef[_]]]

  def register[A](addressKey: String, bindKey: String, clientRef: () ⇒ CloseClientRef[A]): CloseClientRef[A] = {
    addressIndexes.getOrElseUpdate(bindKey, TrieMap[String, CloseClientRef[_]]())
      .getOrElseUpdate(addressKey, clientRef()).asInstanceOf[CloseClientRef[A]]
  }

  def unRegister(bindKey: String): Unit =
    addressIndexes.remove(bindKey).foreach(_.keys.foreach(unRegister(bindKey, _)))

  def unRegister(bindKey: String, addressKey: String): Unit = {
    addressIndexes.get(bindKey).foreach(_.get(addressKey)
      .foreach { clientRef ⇒
        try {
          logger.info(s"client will closed, $bindKey, $addressKey")
          clientRef.close()
        } catch {
          case ex: Exception ⇒ logger.warn(s"Closed client error, $bindKey, $addressKey, ${ex.getMessage}", ex)
        }
      })
  }
}

trait TeleporterContext extends Addresses {
  val indexes: TwoIndexMap[Long, ComponentContext]

  val ref: ActorRef

  def getContext[A <: ComponentContext](id: Long): A = indexes.applyKey1(id).asInstanceOf[A]

  def getContext[A <: ComponentContext](name: String): A = indexes.applyKey2(name).asInstanceOf[A]
}

class TeleporterContextImpl(val ref: ActorRef, val indexes: TwoIndexMap[Long, ComponentContext]) extends TeleporterContext

class TeleporterContextActor(indexes: TwoIndexMap[Long, ComponentContext])(implicit center: TeleporterCenter) extends Actor with Logging {
  override def receive: Receive = contextHandle().orElse(triggerChange).orElse(defaultReceive)

  private def defaultReceive: Receive = {
    case Ping ⇒ sender() ! Pong
  }

  private def contextHandle(): Receive = {
    case Upsert(ctx: ComponentContext, trigger) ⇒
      ctx match {
        case streamContext: StreamContext if streamContext.config.status != StreamStatus.NORMAL ⇒
          self ! Remove(ctx, trigger)
        case _ ⇒
          if (indexes.getKey1(ctx.id).isDefined) {
            self ! Update(ctx, trigger)
          } else {
            self ! Add(ctx, trigger)
          }
      }
    case Add(ctx: ComponentContext, trigger) ⇒
      indexes += (ctx.id, ctx.key, ctx)
      ctx match {
        case _: PartitionContext ⇒
        case taskContext: TaskContext ⇒
          taskContext.config.extraKeys.toMap.values.foreach({ case (_, v: String) ⇒ addLinkKeys(v) })
        case streamContext: StreamContext ⇒
          streamContext.config.extraKeys.toMap.values.foreach({ case (_, v: String) ⇒ addLinkKeys(v) })
        case sourceContext: SourceContext ⇒
          sourceContext.config.extraKeys.toMap.values.foreach({ case (_, v: String) ⇒ addLinkKeys(v) })
          sourceContext.config.addressOption.foreach(addLinkKeys)
        case sinkContext: SinkContext ⇒
          sinkContext.config.extraKeys.toMap.values.foreach({ case (_, v: String) ⇒ addLinkKeys(v) })
          sinkContext.config.addressOption.foreach(addLinkKeys)
        case _: AddressContext ⇒
        case _: VariableContext ⇒
      }
      if (trigger) self ! TriggerAdd(ctx)
    case Update(ctx: ComponentContext, trigger) ⇒
      val oldContext = center.context.getContext[ComponentContext](ctx.id)
      val oldExtraKeys = oldContext.config.extraKeys.toMap.values.collect { case x: String ⇒ x }.toSet
      val extraKeys = ctx.config.extraKeys.toMap.values.collect { case x: String ⇒ x }.toSet
      (oldExtraKeys -- extraKeys).foreach(removeLinkKeys)
      (extraKeys -- oldExtraKeys).foreach(addLinkKeys)
      ctx match {
        case _: PartitionContext ⇒
        case _: TaskContext ⇒
        case _: StreamContext ⇒
        case ctx: SourceContext ⇒
          val oldSourceContext = center.context.getContext[SourceContext](ctx.id)
          if (oldSourceContext.config.address != ctx.config.address) {
            removeLinkKeys(oldSourceContext.config.address)
            addLinkKeys(ctx.config.address)
          }
        case ctx: SinkContext ⇒
          val oldSinkContext = center.context.getContext[SinkContext](ctx.id)
          if (oldSinkContext.config.address != ctx.config.address) {
            removeLinkKeys(oldSinkContext.config.address)
            addLinkKeys(ctx.key)
          }
        case _: AddressContext ⇒
        case _: VariableContext ⇒
      }
      indexes += (ctx.id, ctx.key, ctx)
      if (trigger) self ! TriggerUpdate(ctx)
    case Remove(ctx: ComponentContext, trigger) ⇒
      indexes.removeKey1(ctx.id)
      ctx.config.extraKeys.toMap.values.foreach { case (_, v: String) ⇒ removeLinkKeys(v) }
      ctx match {
        case ctx: PartitionContext ⇒
          ctx.streams().foreach(self ! Remove(_))
        case ctx: TaskContext ⇒ ctx.streams().foreach(self ! Remove(_))
        case ctx: StreamContext ⇒
          ctx.sources().foreach(self ! Remove(_))
          ctx.sinks().foreach(self ! Remove(_))
        case ctx: SourceContext ⇒
          removeLinkKeys(ctx.config.address)
        case ctx: SinkContext ⇒
          removeLinkKeys(ctx.config.address)
        case _: AddressContext ⇒
        case _: VariableContext ⇒
      }
      if (trigger) self ! TriggerRemove(ctx)
    case SyncBroker(ctx) ⇒
      ctx match {
        case ctx: AddressContext ⇒
          center.eventListener.asyncEvent { seqNr ⇒
            center.brokers ! SendMessage(
              TeleporterEvent(seqNr = seqNr, eventType = EventType.LinkAddress, role = Role.CLIENT,
                body = LinkAddress(address = ctx.key, instance = center.instanceKey, keys = ctx.linkKeys.toArray, timestamp = System.currentTimeMillis()))
            )
          }
        case ctx: VariableContext ⇒
          center.eventListener.asyncEvent { seqNr ⇒
            center.brokers ! SendMessage(
              TeleporterEvent(seqNr = seqNr, eventType = EventType.LinkVariable, role = Role.CLIENT,
                body = LinkVariable(variableKey = ctx.key, instance = center.instanceKey, keys = ctx.linkKeys.toArray, timestamp = System.currentTimeMillis()))
            )
          }
      }
  }

  private def addLinkKeys(key: String): Unit = {
    Keys.table(key) match {
      case Tables.address ⇒
        indexes.modifyByKey2(key, {
          case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys + key)
        })
      case Tables.variable ⇒
        indexes.modifyByKey2(key, {
          case variableContext: VariableContext ⇒ variableContext.copy(linkKeys = variableContext.linkKeys + key)
        })
    }
    self ! SyncBroker(indexes.applyKey2(key))
  }

  private def removeLinkKeys(key: String): Unit = {
    Keys.table(key) match {
      case Tables.address ⇒
        indexes.modifyByKey2(key, {
          case addressCtx: AddressContext ⇒ addressCtx.copy(linkKeys = addressCtx.linkKeys - key)
        })
      case Tables.variable ⇒
        indexes.modifyByKey2(key, {
          case variableContext: VariableContext ⇒ variableContext.copy(linkKeys = variableContext.linkKeys - key)
        })
    }
    self ! SyncBroker(indexes.applyKey2(key))
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