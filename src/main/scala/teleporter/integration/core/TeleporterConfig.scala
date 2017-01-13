package teleporter.integration.core

import akka.Done
import akka.actor.{Actor, ActorRef, Props, Status}
import akka.dispatch.Futures
import akka.pattern._
import akka.stream.TeleporterAttribute.SupervisionStrategy
import akka.util.Timeout
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.broker.PersistentProtocol.Keys._
import teleporter.integration.cluster.broker.PersistentProtocol.{KeyValue, Keys, Tables}
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.EventBody.{ConfigChangeNotify, KVGet, KVS}
import teleporter.integration.cluster.rpc._
import teleporter.integration.cluster.rpc.fbs.{EventStatus, EventType, Role}
import teleporter.integration.core.TeleporterContext.Upsert
import teleporter.integration.supervision.StreamDecider
import teleporter.integration.utils.{EventListener, MapBean, MapMetaBean}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Author: kui.dai
  * Date: 2015/11/16.
  */
trait ConfigMetaBean extends MapMetaBean with ConfigMetaBeanFields {

  def id: Long = apply[Long](FId)

  def name: String = this.apply[String](FName)

  def arguments: MapBean = this.get[MapBean](FArguments).getOrElse(MapBean.empty)

  def extraKeys: MapBean = this.get[MapBean](FExtraKeys).getOrElse(MapBean.empty)

  def errorRules: Seq[String] = this.gets[String](FErrorRules)
}

trait ConfigMetaBeanFields {
  val FId = "id"
  val FName = "name"
  val FKey = "key"
  val FArguments = "arguments"
  val FExtraKeys = "extraKeys"
  val FErrorRules = "errorRules"
}

class PartitionMetaBean(val underlying: Map[String, Any]) extends ConfigMetaBean {

  import PartitionMetaBean._

  def keys: Seq[String] = this.gets[String](FKeys)
}

object PartitionMetaBean {
  val FKeys = "keys"
}

class TaskMetaBean(val underlying: Map[String, Any]) extends ConfigMetaBean {
  val FTemplate = "template"

  def template: Option[String] = this.get[String](FTemplate)
}

object StreamStatus {
  val NORMAL = "NORMAL"
  val REMOVE = "REMOVE"
  val COMPLETE = "COMPLETE"
}

class StreamMetaBean(val underlying: Map[String, Any]) extends ConfigMetaBean {

  import StreamMetaBean._

  def template: Option[String] = this.get[String](FTemplate)

  def status: String = apply[String](FStatus)

  def status(status: String): Map[String, Any] = this ++ (FStatus → status)

  def cronOption: Option[String] = this.get[String](FCron)
}

object StreamMetaBean {
  val FTemplate = "template"
  val FStatus = "status"
  val FCron = "cron"
}

class AddressMetaBean(val underlying: Map[String, Any]) extends ConfigMetaBean {

  import AddressMetaBean._

  def category: String = apply[String](FCategory)

  def client: MapBean = this.get[MapBean](FClient).getOrElse(MapBean.empty)

  def share: Boolean = this.get[Boolean](FShare).getOrElse(false)
}

object AddressMetaBean {
  val FCategory = "category"
  val FClient = "client"
  val FShare = "share"
}

class SourceMetaBean(val underlying: Map[String, Any]) extends ConfigMetaBean {

  import SourceMetaBean._

  def category: String = apply[String](FCategory)

  def address: String = apply[String](FAddress)

  def addressOption: Option[String] = this.get[String](FAddress)

  def client: MapBean = this.get[MapBean](FClient).getOrElse(MapBean.empty)

  def shadow: Boolean = this.get[Boolean](FShadow).exists(b ⇒ b)
}

object SourceMetaBean {
  val FCategory = "category"
  val FAddress = "address"
  val FClient = "client"
  val FShadow = "shadow"
}

class SinkMetaBean(val underlying: Map[String, Any]) extends ConfigMetaBean {

  import SinkMetaBean._

  def category: String = apply[String](FCategory)

  def address: String = apply[String](FAddress)

  def addressOption: Option[String] = this.get[String](FAddress)

  def client: MapBean = this.get[MapBean](FClient).getOrElse(MapBean.empty)
}

object SinkMetaBean {
  val FCategory = "category"
  val FAddress = "address"
  val FClient = "client"
}

class VariableMetaBean(val underlying: Map[String, Any]) extends ConfigMetaBean

object TeleporterConfigActor {

  case class LoadPartition(key: String, trigger: Boolean = true)

  case class LoadTask(key: String, trigger: Boolean = true)

  case class LoadStream(key: String, trigger: Boolean = true)

  case class LoadStreams(key: String, trigger: Boolean = true)

  case class LoadSource(key: String, trigger: Boolean = true)

  case class LoadSources(key: String, trigger: Boolean = true)

  case class LoadSink(key: String, trigger: Boolean = true)

  case class LoadSinks(key: String, trigger: Boolean = true)

  case class LoadAddress(key: String, trigger: Boolean = true)

  case class LoadVariable(key: String, trigger: Boolean = true)

  case class LoadExtra(key: String, trigger: Boolean = true)

  def apply(eventListener: EventListener[TeleporterEvent[_ <: EventBody]])
           (implicit center: TeleporterCenter): ActorRef = {
    center.system.actorOf(Props(classOf[TeleporterConfigActor], eventListener, center), "teleporter-config")
  }
}

class TeleporterConfigActor(eventListener: EventListener[TeleporterEvent[_ <: EventBody]])
                           (implicit center: TeleporterCenter) extends Actor with Logging {

  import TeleporterConfigActor._
  import context.dispatcher

  implicit val timeout: Timeout = 2.minutes

  override def receive: Receive = {
    case LoadPartition(key, trigger) ⇒ loadPartition(key, trigger) pipeTo sender()
    case LoadTask(key, trigger) ⇒ loadTask(key, trigger) pipeTo sender()
    case LoadStream(key, trigger) ⇒ loadStream(key, trigger) pipeTo sender()
    case LoadStreams(key, trigger) ⇒ loadStreams(key, trigger) pipeTo sender()
    case LoadSource(key, trigger) ⇒ loadSource(key, trigger) pipeTo sender()
    case LoadSources(key, trigger) ⇒ loadSources(key, trigger) pipeTo sender()
    case LoadSink(key, trigger) ⇒ loadSink(key, trigger) pipeTo sender()
    case LoadSinks(key, trigger) ⇒ loadSinks(key, trigger) pipeTo sender()
    case LoadAddress(key, trigger) ⇒ loadAddress(key, trigger) pipeTo sender()
    case LoadExtra(key, trigger) ⇒ loadExtraKey(key, trigger) pipeTo sender()
    case Status.Failure(e) ⇒ logger.error(e.getMessage, e)
  }

  private def loadPartition(key: String, trigger: Boolean = true): Future[PartitionContext] = {
    center.client.getConfig(key).flatMap { kv ⇒
      val km = kv.metaBean[PartitionMetaBean]
      val partitionId = km.value.id
      val partitionContext = PartitionContext(id = partitionId, key = km.key, config = km.value)
      val taskKey = Keys.mapping(key, PARTITION, TASK)
      for {
        _ ← self ? LoadTask(taskKey, trigger = false)
        _ ← Futures.sequence(km.value.keys.map {
          key ⇒ self ? LoadStreams(key, trigger = false)
        }.asInstanceOf[Seq[Future[Seq[StreamContext]]]].asJava, dispatcher).map(_.asScala.flatten)
      } yield {
        center.context.ref ! Upsert(partitionContext, trigger)
        partitionContext
      }
    }
  }

  private def loadTask(key: String, trigger: Boolean = true): Future[TaskContext] = {
    center.client.getConfig(key).flatMap { kv ⇒
      val km = kv.metaBean[TaskMetaBean]
      val taskId = km.value.id
      val taskContext = TaskContext(id = taskId, key = km.key, config = km.value)
      for {
        _ ← Futures.sequence(km.value.extraKeys.toMap.map { case (_, v: String) ⇒ self ? LoadExtra(v, trigger) }
          .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
      } yield {
        center.context.ref ! Upsert(taskContext, trigger)
        taskContext
      }
    }
  }

  private def loadStream(key: String, trigger: Boolean = true): Future[StreamContext] = {
    center.client.getConfig(key).flatMap { kv ⇒
      val km = kv.metaBean[StreamMetaBean]
      val streamId = km.value.id
      val streamContext = StreamContext(id = streamId, key = km.key, config = km.value,
        decider = new StreamDecider(key = key, streamRef = center.streams, SupervisionStrategy(key, km.value)))
      for {
        _ ← self ? LoadSources(Keys.mapping(key, STREAM, STREAM_SOURCES), trigger = false)
        _ ← self ? LoadSinks(Keys.mapping(key, STREAM, STREAM_SINKS), trigger = false)
        _ ← Futures.sequence(km.value.extraKeys.toMap.map { case (_, v: String) ⇒ self ? LoadExtra(v, trigger) }
          .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
      } yield {
        center.context.ref ! Upsert(streamContext, trigger)
        streamContext
      }
    }
  }

  private def loadStreams(key: String, trigger: Boolean = true): Future[Seq[StreamContext]] = {
    center.client.getRangeRegexConfig(key).flatMap { kvs ⇒
      val streamContexts = kvs.map { kv ⇒
        val km = kv.metaBean[StreamMetaBean]
        val streamId = km.value.id
        val streamContext = StreamContext(id = streamId, key = km.key, config = km.value,
          decider = new StreamDecider(key = key, streamRef = center.streams, SupervisionStrategy(key, km.value)))
        for {
          _ ← self ? LoadSources(Keys.mapping(kv.key, STREAM, STREAM_SOURCES), trigger = false)
          _ ← self ? LoadSinks(Keys.mapping(kv.key, STREAM, STREAM_SINKS), trigger = false)
          _ ← Futures.sequence(km.value.extraKeys.toMap.map { case (_, v: String) ⇒ self ? LoadExtra(v, trigger) }
            .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
        } yield {
          center.context.ref ! Upsert(streamContext, trigger)
          streamContext
        }
      }
      Futures.sequence(streamContexts.asJava, dispatcher)
    }.map(_.asScala.toSeq)
  }

  private def loadSource(key: String, trigger: Boolean = true): Future[SourceContext] = {
    center.client.getConfig(key).flatMap { kv ⇒
      val km = kv.metaBean[SourceMetaBean]
      val sourceId = km.value.id
      val sourceContext = SourceContext(id = sourceId, key = km.key, config = km.value)
      km.value.addressOption match {
        case Some(addressKey) if addressKey.nonEmpty ⇒
          for {
            _ ← self ? LoadAddress(addressKey, trigger = false)
            _ ← Futures.sequence(km.value.extraKeys.toMap.map { case (_, v: String) ⇒ self ? LoadExtra(v, trigger) }
              .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
          } yield {
            center.context.ref ! Upsert(sourceContext, trigger)
            sourceContext
          }
        case _ ⇒ Future.successful(sourceContext)
      }
    }
  }

  private def loadSources(key: String, trigger: Boolean = true): Future[Seq[SourceContext]] = {
    center.client.getRangeRegexConfig(key).flatMap { kvs ⇒
      val sourceContexts = kvs.map { kv ⇒
        val km = kv.metaBean[SourceMetaBean]
        val sourceId = km.value.id
        val sourceContext = SourceContext(id = sourceId, key = km.key, config = km.value)
        km.value.addressOption match {
          case Some(addressKey) if addressKey.nonEmpty ⇒
            for {
              _ ← self ? LoadAddress(addressKey, trigger = false)
              _ ← Futures.sequence(km.value.extraKeys.toMap.map { case (_, v: String) ⇒ self ? LoadExtra(v, trigger) }
                .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
            } yield {
              center.context.ref ! Upsert(sourceContext, trigger)
              sourceContext
            }
          case _ ⇒ Future.successful(sourceContext)
        }
      }
      Futures.sequence(sourceContexts.asJava, dispatcher)
    }.map(_.asScala.toSeq)
  }

  private def loadSink(key: String, trigger: Boolean = true): Future[SinkContext] = {
    center.client.getConfig(key).flatMap { kv ⇒
      val km = kv.metaBean[SinkMetaBean]
      val sinkId = km.value.id
      val sinkContext = SinkContext(id = sinkId, key = km.key, config = km.value)
      km.value.addressOption match {
        case Some(addressKey) if addressKey.nonEmpty ⇒
          for {
            _ ← self ? LoadAddress(addressKey, trigger = false)
            _ ← Futures.sequence(km.value.extraKeys.toMap.map { case (_, v: String) ⇒ self ? LoadExtra(v, trigger) }
              .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
          } yield {
            center.context.ref ! Upsert(sinkContext, trigger)
            sinkContext
          }
        case _ ⇒ Future.successful(sinkContext)
      }
    }
  }

  private def loadSinks(key: String, trigger: Boolean = true): Future[Seq[SinkContext]] = {
    center.client.getRangeRegexConfig(key).flatMap { kvs ⇒
      val sinkContexts = kvs.map { kv ⇒
        val km = kv.metaBean[SinkMetaBean]
        val sinkId = km.value.id
        val sinkContext = SinkContext(id = sinkId, key = km.key, config = km.value)
        km.value.addressOption match {
          case Some(addressKey) ⇒
            for {
              _ ← self ? LoadAddress(addressKey, trigger = false)
              _ ← Futures.sequence(km.value.extraKeys.toMap.map { case (_, v: String) ⇒ self ? LoadExtra(v, trigger) }
                .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
            } yield {
              center.context.ref ! Upsert(sinkContext, trigger)
              sinkContext
            }
          case None ⇒ Future.successful(sinkContext)
        }
      }
      Futures.sequence(sinkContexts.asJava, dispatcher)
    }.map(_.asScala.toSeq)
  }

  private def loadExtraKey(key: String, trigger: Boolean = true): Future[ComponentContext] = {
    Keys.table(key) match {
      case Tables.address ⇒
        loadAddress(key, trigger)
      case Tables.variable ⇒
        loadVariable(key, trigger)
    }
  }

  private def loadAddress(key: String, trigger: Boolean = true): Future[AddressContext] = {
    center.client.getConfig(key).map { kv ⇒
      val km = kv.metaBean[AddressMetaBean]
      val addressId = km.value.id
      val addressContext = AddressContext(id = addressId, key = km.key, config = km.value, linkKeys = Set.empty)
      center.context.ref ! Upsert(addressContext, trigger)
      addressContext
    }
  }

  private def loadVariable(key: String, trigger: Boolean = true): Future[VariableContext] = {
    center.client.getConfig(key).map { kv ⇒
      val km = kv.metaBean[VariableMetaBean]
      val variableId = km.value.id
      val variableContext = VariableContext(id = variableId, key = km.key, config = km.value, linkKeys = Set.empty)
      center.context.ref ! Upsert(variableContext, trigger)
      variableContext
    }
  }
}

trait TeleporterConfigClient {
  val center: TeleporterCenter

  import center.system.dispatcher

  def getConfig(key: String): Future[KeyValue] = {
    require(key.nonEmpty, "Key can't empty to load")
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.KVGet, role = Role.Request, body = KVGet(key))
      )
    }._2.map(e ⇒ e.toBody[EventBody.KV].keyValue)
  }

  def getRangeRegexConfig(key: String, start: Int = 0, limit: Int = Int.MaxValue): Future[Seq[KeyValue]] = {
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.RangeRegexKV, role = Role.Request,
          body = EventBody.RangeRegexKV(key = key, start = start, limit = limit))
      )
    }._2.map(e ⇒ e.toBody[KVS].kvs.map(kv ⇒ KeyValue(kv.key, kv.value)))
  }

  def save(key: String, value: String): Future[Done] = {
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.KVSave, role = Role.Request,
          body = EventBody.KV(key = key, value = value))
      )
    }._2.map {
      case e if e.status == EventStatus.Success ⇒ Done
      case _ ⇒ throw new RuntimeException(s"$key, value save failed!")
    }
  }

  def atomicSave(key: String, expect: String, update: String): Future[Boolean] = {
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.KVSave, role = Role.Request,
          body = EventBody.AtomicKV(key = key, expect = expect, update = update))
      )
    }._2.map {
      case e if e.status == EventStatus.Success ⇒ true
      case _ ⇒ false
    }
  }

  def remove(key: String): Future[Done] = {
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.KVRemove, role = Role.Request,
          body = EventBody.KVRemove(key = key))
      )
    }._2.map {
      case e if e.status == EventStatus.Success ⇒ Done
      case _ ⇒ throw new RuntimeException(s"$key, value save failed!")
    }
  }

  def notify(key: String, action: Byte): Future[Done] = {
    center.eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent(seqNr = seqNr, eventType = EventType.ConfigChangeNotify, role = Role.Request,
          body = ConfigChangeNotify(key = key, action = action, timestamp = System.currentTimeMillis()))
      )
    }._2.map {
      case e if e.status == EventStatus.Success ⇒ Done
      case _ ⇒ throw new RuntimeException(s"$key, value save failed!")
    }
  }
}

class TeleporterConfigClientImpl()(implicit val center: TeleporterCenter) extends TeleporterConfigClient

object TeleporterConfigClient {
  def apply()(implicit center: TeleporterCenter): TeleporterConfigClient = new TeleporterConfigClientImpl()
}