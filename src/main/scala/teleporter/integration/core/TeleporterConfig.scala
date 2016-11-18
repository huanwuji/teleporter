package teleporter.integration.core

import akka.actor.{Actor, ActorRef, Props}
import akka.dispatch.Futures
import akka.pattern._
import akka.util.Timeout
import teleporter.integration.cluster.broker.PersistentProtocol.Keys._
import teleporter.integration.cluster.broker.PersistentProtocol.{KeyValue, Keys, Tables}
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, KV, KVS, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.broker.Broker.{KVGet, RangeRegexKV}
import teleporter.integration.core.TeleporterContext.Upsert
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{EventListener, MapBean, MapMetadata}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Author: kui.dai
  * Date: 2015/11/16.
  */
trait ConfigMetadata extends MapMetadata {
  val FId = "id"
  val FName = "name"
  val FKey = "key"
  val FCategory = "category"
  val FArguments = "arguments"
  val FAddress = "address"
  val FErrorRules = "errorRules"

  def lnsId(implicit bean: MapBean): Long = bean[Long](FId)

  def lnsName(implicit bean: MapBean): String = bean[String](FName)

  def lnsCategory(implicit bean: MapBean): String = bean[String](FCategory)

  def lnsArguments(implicit bean: MapBean): MapBean = bean.__dict__[MapBean](FArguments).getOrElse(MapBean.empty)

  def lnsAddress(implicit bean: MapBean): String = bean[String](FAddress)
}

trait TaskMetadata extends ConfigMetadata {
  val FTemplate = "template"
  val FStreams = "streams"
}

object TaskMetadata extends TaskMetadata

trait StreamMetadata extends ConfigMetadata {
  val FTemplate = "template"
  val FStatus = "status"
  val FCron = "cron"

  def lnsStatus(implicit bean: MapBean): String = bean[String](FStatus)

  def lnsCron(implicit bean: MapBean): String = bean[String](FCron)
}

object StreamMetadata extends StreamMetadata

object StreamStatus {
  val NORMAL = "NORMAL"
  val REMOVE = "REMOVE"
  val COMPLETE = "COMPLETE"
}

trait AddressMetadata extends ConfigMetadata {
  val FClient = "client"

  def lnsClient(implicit bean: MapBean): MapBean = bean.__dict__[MapBean](FClient).getOrElse(MapBean.empty)
}

trait SourceMetadata extends ConfigMetadata {
  val FClient = "client"
  val FShadow = "shadow"

  def lnsClient(implicit bean: MapBean): MapBean = bean.__dict__[MapBean](FClient).getOrElse(MapBean.empty)

  def lnsShadow(implicit bean: MapBean): Boolean = bean.__dict__[Boolean](FShadow).exists(b ⇒ b)
}

trait SinkMetadata extends ConfigMetadata {
  val FClient = "client"
  val FClientParallelism = "parallelism"

  def lnsClient(implicit bean: MapBean): MapBean = bean.__dict__[MapBean](FClient).getOrElse(MapBean.empty)

  def lnsParallelism(implicit bean: MapBean): Int = lnsClient.__dict__[Int](FClientParallelism).getOrElse(0)
}

trait VariableMetadata extends ConfigMetadata

trait TeleporterConfig extends MapBean {
  def id() = apply[Long]("id")
}

class TeleporterConfigImpl(val underlying: Map[String, Any]) extends TeleporterConfig

object TeleporterConfig {
  type TaskConfig = TeleporterConfig
  type PartitionConfig = TeleporterConfig
  type StreamConfig = TeleporterConfig
  type AddressConfig = TeleporterConfig
  type SourceConfig = TeleporterConfig
  type SinkConfig = TeleporterConfig
  type VariableConfig = TeleporterConfig

  implicit def apply(bean: Map[String, Any]): TeleporterConfig = new TeleporterConfigImpl(bean)
}

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

  def apply(eventListener: EventListener[TeleporterEvent])
           (implicit center: TeleporterCenter): ActorRef = {
    center.system.actorOf(Props(classOf[TeleporterConfigActor], eventListener, center), "teleporter-config")
  }
}

class TeleporterConfigActor(eventListener: EventListener[TeleporterEvent])
                           (implicit center: TeleporterCenter) extends Actor {

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
  }

  private def loadPartition(key: String, trigger: Boolean = true): Future[PartitionContext] = {
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val partitionId = km.value[Long]("id")
      val partitionContext = PartitionContext(id = partitionId, key = km.key, config = km.value)
      val tasKey = Keys.mapping(key, PARTITION, TASK)
      for {
        task ← self ? LoadTask(tasKey, trigger = false)
        streamContext ← Futures.sequence(km.value.__dicts__[String]("extraKeys").map {
          key ⇒ (self ? LoadStreams(key, trigger = false)).asInstanceOf[Future[Seq[StreamContext]]]
        }.asJava, dispatcher).map(_.asScala.flatten)
      } yield {
        center.context.ref ! Upsert(partitionContext, trigger)
        partitionContext
      }
    }
  }

  private def loadTask(key: String, trigger: Boolean = true): Future[TaskContext] = {
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val taskId = km.value[Long]("id")
      val taskContext = TaskContext(id = taskId, key = km.key, config = km.value)
      for {
        extraKeys ← Futures.sequence(taskContext.config.__dicts__[String]("extraKeys").map(key ⇒ self ? LoadExtra(key, trigger))
          .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
      } yield {
        center.context.ref ! Upsert(taskContext, trigger)
        taskContext
      }
    }
  }

  private def loadStream(key: String, trigger: Boolean = true): Future[StreamContext] = {
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val streamId = km.value[Long]("id")
      val streamContext = StreamContext(id = streamId, key = km.key, config = km.value)
      for {
        sourceContexts ← self ? LoadSources(Keys.mapping(key, STREAM, STREAM_SOURCES), trigger = false)
        sinkContexts ← self ? LoadSinks(Keys.mapping(key, STREAM, STREAM_SINKS), trigger = false)
        extraKeys ← Futures.sequence(streamContext.config.__dicts__[String]("extraKeys").map(key ⇒ self ? LoadExtra(key, trigger))
          .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
      } yield {
        center.context.ref ! Upsert(streamContext, trigger)
        streamContext
      }
    }
  }

  private def loadStreams(key: String, trigger: Boolean = true): Future[Seq[StreamContext]] = {
    getRangeRegexConfig(key).flatMap { kvs ⇒
      val streamContexts = kvs.map { kv ⇒
        val km = kv.config
        val streamId = km.value[Long]("id")
        val streamContext = StreamContext(id = streamId, key = km.key, config = km.value)
        for {
          sourceContexts ← self ? LoadSources(Keys.mapping(kv.key, STREAM, STREAM_SOURCES), trigger = false)
          sinkContexts ← self ? LoadSinks(Keys.mapping(kv.key, STREAM, STREAM_SINKS), trigger = false)
          extraKeys ← Futures.sequence(streamContext.config.__dicts__[String]("extraKeys").map(key ⇒ self ? LoadExtra(key, trigger))
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
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val sourceId = km.value[Long]("id")
      val sourceContext = SourceContext(id = sourceId, key = km.key, config = km.value)
      km.value.__dict__[String]("address") match {
        case Some(addressKey) ⇒
          for {
            addressContext ← self ? LoadAddress(sourceContext.addressKey, trigger = false)
            extraKeys ← Futures.sequence(sourceContext.config.__dicts__[String]("extraKeys").map(key ⇒ self ? LoadExtra(key, trigger))
              .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
          } yield {
            center.context.ref ! Upsert(sourceContext, trigger)
            sourceContext
          }
        case None ⇒ Future.successful(sourceContext)
      }
    }
  }

  private def loadSources(key: String, trigger: Boolean = true): Future[Seq[SourceContext]] = {
    getRangeRegexConfig(key).flatMap { kvs ⇒
      val sourceContexts = kvs.map { kv ⇒
        val km = kv.config
        val sourceId = km.value[Long]("id")
        val sourceContext = SourceContext(id = sourceId, key = km.key, config = km.value)
        km.value.__dict__[String]("address") match {
          case Some(addressKey) ⇒
            for {
              addressContext ← self ? LoadAddress(sourceContext.addressKey, trigger = false)
              extraKeys ← Futures.sequence(sourceContext.config.__dicts__[String]("extraKeys").map(key ⇒ self ? LoadExtra(key, trigger))
                .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
            } yield {
              center.context.ref ! Upsert(sourceContext, trigger)
              sourceContext
            }
          case None ⇒ Future.successful(sourceContext)
        }
      }
      Futures.sequence(sourceContexts.asJava, dispatcher)
    }.map(_.asScala.toSeq)
  }

  private def loadSink(key: String, trigger: Boolean = true): Future[SinkContext] = {
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val sinkId = km.value[Long]("id")
      val sinkContext = SinkContext(id = sinkId, key = km.key, config = km.value)
      km.value.__dict__[String]("address") match {
        case Some(addressKey) ⇒
          for {
            addressContext ← self ? LoadAddress(sinkContext.addressKey, trigger = false)
            extraKeys ← Futures.sequence(sinkContext.config.__dicts__[String]("extraKeys").map(key ⇒ self ? LoadExtra(key, trigger))
              .asInstanceOf[Seq[Future[ComponentContext]]].asJava, context.dispatcher)
          } yield {
            center.context.ref ! Upsert(sinkContext, trigger)
            sinkContext
          }
        case None ⇒ Future.successful(sinkContext)
      }
    }
  }

  private def loadSinks(key: String, trigger: Boolean = true): Future[Seq[SinkContext]] = {
    getRangeRegexConfig(key).flatMap { kvs ⇒
      val sinkContexts = kvs.map { kv ⇒
        val km = kv.config
        val sinkId = km.value[Long]("id")
        val sinkContext = SinkContext(id = sinkId, key = km.key, config = km.value)
        km.value.__dict__[String]("address") match {
          case Some(addressKey) ⇒
            for {
              addressContext ← self ? LoadAddress(sinkContext.addressKey, trigger = false)
              extraKeys ← Futures.sequence(sinkContext.config.__dicts__[String]("extraKeys").map(key ⇒ self ? LoadExtra(key, trigger))
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
    getConfig(key).map { kv ⇒
      val km = kv.config
      val addressId = km.value[Long]("id")
      val share = km.value.__dict__[Boolean]("share").getOrElse(false)
      val addressContext = AddressContext(id = addressId, key = km.key, config = km.value, linkKeys = Set.empty, clientRefs = ClientRefs(share))
      center.context.ref ! Upsert(addressContext, trigger)
      addressContext
    }
  }

  private def loadVariable(key: String, trigger: Boolean = true): Future[VariableContext] = {
    getConfig(key).map { kv ⇒
      val km = kv.config
      val variableId = km.value[Long]("id")
      val variableContext = VariableContext(id = variableId, key = km.key, config = km.value, linkKeys = Set.empty)
      center.context.ref ! Upsert(variableContext, trigger)
      variableContext
    }
  }

  private def getConfig(key: String): Future[KeyValue] = {
    eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent.newBuilder()
          .setRole(TeleporterEvent.Role.CLIENT)
          .setType(EventType.KVGet)
          .setSeqNr(seqNr)
          .setBody(KVGet.newBuilder().setKey(key).build().toByteString).build()
      )
    }._2.map(e ⇒ KV.parseFrom(e.getBody)).map(kv ⇒ KeyValue(kv.getKey, kv.getValue))
  }

  private def getRangeRegexConfig(key: String, start: Int = 0, limit: Int = Int.MaxValue): Future[Seq[KeyValue]] = {
    eventListener.asyncEvent { seqNr ⇒
      center.brokers ! SendMessage(
        TeleporterEvent.newBuilder()
          .setRole(TeleporterEvent.Role.CLIENT)
          .setType(EventType.RangeRegexKV)
          .setSeqNr(seqNr)
          .setBody(RangeRegexKV.newBuilder().setKey(key).setStart(start).setLimit(limit).build().toByteString).build()
      )
    }._2.map(e ⇒ KVS.parseFrom(e.getBody).getKvsList.asScala)
      .map(kvs ⇒ kvs.map(kv ⇒ KeyValue(kv.getKey, kv.getValue)))
  }
}