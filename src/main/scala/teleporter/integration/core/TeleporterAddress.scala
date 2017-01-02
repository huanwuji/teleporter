package teleporter.integration.core

import teleporter.integration.ClientApply
import teleporter.integration.component.hbase.HbaseComponent
import teleporter.integration.component.hdfs.HdfsComponent
import teleporter.integration.component.jdbc.JdbcComponent
import teleporter.integration.component.mongo.MongoComponent
import teleporter.integration.component.taobao.TaobaoComponent
import teleporter.integration.component.{ElasticComponent, _}

import scala.collection.concurrent.TrieMap

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait ClientRef[+A] {
  val key: String
  val client: A
}

class CloseClientRef[A](val key: String, val client: A, closeHandler: CloseClientRef[A] ⇒ Unit) extends ClientRef[A] with AutoCloseable {
  override def close(): Unit = closeHandler(this)
}

case class AutoCloseClientRef[A <: AutoCloseable](override val key: String, override val client: A) extends CloseClientRef[A](key, client, _.client.close())

trait TeleporterAddress {

  private val clientApplies = TrieMap[String, ClientApply](
    "kafka_producer" → KafkaComponent.kafkaProducerApply,
    "kafka_consumer" → KafkaComponent.kafkaConsumerApply,
    "jdbc" → JdbcComponent.jdbcApply,
    "hbase.common" → HbaseComponent.hbaseApply,
    "elasticsearch" → ElasticComponent.elasticClientApply,
    "mongo" → MongoComponent.mongoApply,
    "influxdb" → InfluxdbComponent.influxdbApply,
    "hdfs" → HdfsComponent.hdfsApply,
    "taobao" → TaobaoComponent.taobaoClientApply
  )

  def apply[A](key: String)(implicit center: TeleporterCenter): A = {
    val context = center.context.getContext[AddressContext](key)
    context.clientRefs(key, clientApplies(context.config.category)).asInstanceOf[A]
  }

  def registerType(category: String, factory: ClientApply): Unit = {
    clientApplies += category → factory
  }
}

class TeleporterAddressImpl extends TeleporterAddress

object TeleporterAddress {
  def apply(): TeleporterAddress = new TeleporterAddressImpl
}