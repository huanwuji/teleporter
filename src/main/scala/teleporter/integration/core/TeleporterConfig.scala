package teleporter.integration.core

import akka.dispatch.Futures
import teleporter.integration.cluster.broker.PersistentProtocol.Keys._
import teleporter.integration.cluster.broker.PersistentProtocol.Values.PartitionValue
import teleporter.integration.cluster.broker.PersistentProtocol.{KeyValue, Keys}
import teleporter.integration.cluster.instance.Brokers.SendMessage
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, KV, KVS, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.broker.Broker.{KVGet, RangeRegexKV}
import teleporter.integration.core.TeleporterContext.Upsert
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{EventListener, MapBean, MapMetadata}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

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
}

object StreamMetadata extends StreamMetadata

trait AddressMetadata extends ConfigMetadata {
  val FClient = "client"

  def lnsClient(implicit bean: MapBean): MapBean = bean.__dict__[MapBean](FClient).getOrElse(MapBean.empty)
}

trait SourceMetadata extends ConfigMetadata {
  val FShareAddressInstance = "shareAddressInstance"
  val FClient = "client"
  val FShadow = "shadow"

  def lnsClient(implicit bean: MapBean): MapBean = bean.__dict__[MapBean](FClient).getOrElse(MapBean.empty)

  def lnsShareAddressInstance(implicit bean: MapBean): Boolean = bean.__dict__[Boolean](FShareAddressInstance).exists(b ⇒ b)

  def lnsShadow(implicit bean: MapBean): Boolean = bean.__dict__[Boolean](FShadow).exists(b ⇒ b)
}

trait SinkMetadata extends ConfigMetadata {
  val FPrototype = "prototype"
  val FParallelism = "parallelism"

  def lnsParallelism(implicit bean: MapBean): Int = bean[Int](FParallelism)
}

trait VariableMetadata extends ConfigMetadata


trait TeleporterConfig extends MapBean {
  def id() = apply[Long]("id")
}

class TeleporterConfigImpl(val underlying: Map[String, Any]) extends TeleporterConfig

object TeleporterConfig {
  implicit def apply(bean: Map[String, Any]): TeleporterConfig = new TeleporterConfigImpl(bean)

  type TaskConfig = TeleporterConfig
  type PartitionConfig = TeleporterConfig
  type StreamConfig = TeleporterConfig
  type AddressConfig = TeleporterConfig
  type SourceConfig = TeleporterConfig
  type SinkConfig = TeleporterConfig
  type VariableConfig = TeleporterConfig
}

trait TeleporterConfigService {
  val eventListener: EventListener[TeleporterEvent]
  val center: TeleporterCenter
  implicit val ec: ExecutionContext

  def loadPartition(key: String, trigger: Boolean = true): Future[PartitionContext] = {
    getConfig(key).flatMap { kv ⇒
      val kb = kv.keyBean[PartitionValue]
      val partitionId = kb.value.id
      val partitionContext = PartitionContext(id = partitionId, key = kb.key, config = kb.value)
      val tasKey = Keys.mapping(key, PARTITION, TASK)
      for {
        task ← loadTask(tasKey, trigger = false)
        streamContext ← Futures.sequence(kb.value.keys.map {
          key ⇒ loadStreams(key, trigger = false)
        }.asJava, ec).map(_.asScala.flatten)
      } yield {
        center.context.ref ! Upsert(partitionContext, trigger)
        partitionContext
      }
    }
  }

  def loadTask(key: String, trigger: Boolean = true): Future[TaskContext] = {
    getConfig(key).map { kv ⇒
      val km = kv.config
      val taskId = km.value[Long]("id")
      val taskContext = TaskContext(id = taskId, key = km.key, config = km.value, variableKeys = Set.empty)
      center.context.ref ! Upsert(taskContext, trigger)
      taskContext
    }
  }

  def loadStream(key: String, trigger: Boolean = true): Future[StreamContext] = {
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val streamId = km.value[Long]("id")
      val streamContext = StreamContext(id = streamId, key = km.key, config = km.value, variableKeys = Set.empty)
      for {
        sourceContexts ← loadSources(Keys.mapping(key, STREAM, STREAM_SOURCES), trigger = false)
        sinkContexts ← loadSinks(Keys.mapping(key, STREAM, STREAM_SINKS), trigger = false)
      } yield {
        center.context.ref ! Upsert(streamContext, trigger)
        streamContext
      }
    }
  }

  def loadStreams(key: String, trigger: Boolean = true): Future[Seq[StreamContext]] = {
    getRangeRegexConfig(key).flatMap { kvs ⇒
      val streamContexts = kvs.map { kv ⇒
        val km = kv.config
        val streamId = km.value[Long]("id")
        val streamContext = StreamContext(id = streamId, key = km.key, config = km.value, variableKeys = Set.empty)
        for {
          sourceContexts ← loadSources(Keys.mapping(kv.key, STREAM, STREAM_SOURCES), trigger = false)
          sinkContexts ← loadSinks(Keys.mapping(kv.key, STREAM, STREAM_SINKS), trigger = false)
        } yield {
          center.context.ref ! Upsert(streamContext, trigger)
          streamContext
        }
      }
      Futures.sequence(streamContexts.asJava, ec)
    }.map(_.asScala.toSeq)
  }

  def loadSource(key: String, trigger: Boolean = true): Future[SourceContext] = {
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val sourceId = km.value[Long]("id")
      val sourceContext = SourceContext(id = sourceId, key = km.key, config = km.value, variableKeys = Set.empty)
      km.value.__dict__[String]("address") match {
        case Some(addressKey) ⇒
          for {
            addressContext ← loadAddress(sourceContext.addressKey, trigger = false)
          } yield {
            center.context.ref ! Upsert(sourceContext, trigger)
            sourceContext
          }
        case None ⇒ Future.successful(sourceContext)
      }
    }
  }

  def loadSources(key: String, trigger: Boolean = true): Future[Seq[SourceContext]] = {
    getRangeRegexConfig(key).flatMap { kvs ⇒
      val sourceContexts = kvs.map { kv ⇒
        val km = kv.config
        val sourceId = km.value[Long]("id")
        val sourceContext = SourceContext(id = sourceId, key = km.key, config = km.value, variableKeys = Set.empty)
        km.value.__dict__[String]("address") match {
          case Some(addressKey) ⇒
            for {
              addressContext ← loadAddress(sourceContext.addressKey, trigger = false)
            } yield {
              center.context.ref ! Upsert(sourceContext, trigger)
              sourceContext
            }
          case None ⇒ Future.successful(sourceContext)
        }
      }
      Futures.sequence(sourceContexts.asJava, ec)
    }.map(_.asScala.toSeq)
  }

  def loadSink(key: String, trigger: Boolean = true): Future[SinkContext] = {
    getConfig(key).flatMap { kv ⇒
      val km = kv.config
      val sinkId = km.value[Long]("id")
      val sinkContext = SinkContext(id = sinkId, key = km.key, config = km.value, variableKeys = Set.empty)
      km.value.__dict__[String]("address") match {
        case Some(addressKey) ⇒
          for {
            addressContext ← loadAddress(sinkContext.addressKey, trigger = false)
          } yield {
            center.context.ref ! Upsert(sinkContext, trigger)
            sinkContext
          }
        case None ⇒ Future.successful(sinkContext)
      }
    }
  }

  def loadSinks(key: String, trigger: Boolean = true): Future[Seq[SinkContext]] = {
    getRangeRegexConfig(key).flatMap { kvs ⇒
      val sinkContexts = kvs.map { kv ⇒
        val km = kv.config
        val sinkId = km.value[Long]("id")
        val sinkContext = SinkContext(id = sinkId, key = km.key, config = km.value, variableKeys = Set.empty)
        km.value.__dict__[String]("address") match {
          case Some(addressKey) ⇒
            for {
              addressContext ← loadAddress(sinkContext.addressKey, trigger = false)
            } yield {
              center.context.ref ! Upsert(sinkContext, trigger)
              sinkContext
            }
          case None ⇒ Future.successful(sinkContext)
        }
      }
      Futures.sequence(sinkContexts.asJava, ec)
    }.map(_.asScala.toSeq)
  }

  def loadAddress(key: String, trigger: Boolean = true): Future[AddressContext] = {
    getConfig(key).map { kv ⇒
      val km = kv.config
      val addressId = km.value[Long]("id")
      val share = km.value[Boolean]("share")
      val addressContext = AddressContext(id = addressId, key = km.key, config = km.value, linkKeys = Set.empty, variableKeys = Set.empty, clientRefs = ClientRefs(share))
      center.context.ref ! Upsert(addressContext, trigger)
      addressContext
    }
  }

  def getConfig(key: String): Future[KeyValue] = {
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

  def getRangeRegexConfig(key: String, start: Int = 0, limit: Int = Int.MaxValue): Future[Seq[KeyValue]] = {
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

class TeleporterConfigServiceImpl(val eventListener: EventListener[TeleporterEvent]
                                 )(implicit val center: TeleporterCenter, val ec: ExecutionContext) extends TeleporterConfigService

object TeleporterConfigService {
  def apply(eventListener: EventListener[TeleporterEvent])
           (implicit center: TeleporterCenter, ec: ExecutionContext): TeleporterConfigService = {
    new TeleporterConfigServiceImpl(eventListener)(center, ec)
  }
}