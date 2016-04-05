package teleporter.integration.core

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.reflect.ConstructorUtils
import teleporter.integration.CmpType
import teleporter.integration.component._
import teleporter.integration.component.hbase.HbaseAddressBuilder
import teleporter.integration.component.jdbc.DataSourceAddressBuilder
import teleporter.integration.component.mongo.MongoAddressBuilder
import teleporter.integration.conf.Conf
import teleporter.integration.core.conf.TeleporterConfigFactory

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
trait AddressBuilder[A] {
  def center: TeleporterCenter

  def conf: Conf.Address

  def build: Address[A]
}

trait Address[A] {
  val conf: Conf.Address
  val client: A

  def close(): Unit = {}
}

class AutoCloseAddress[A <: AutoCloseable](val conf: Conf.Address, val client: A) extends Address[A] {
  override def close(): Unit = client.close()
}

case class AddressRef(cmpType: CmpType, id: Int)

object AddressRef {
  def apply(ref: Any): AddressRef = {
    ref match {
      case source: Conf.Source ⇒ AddressRef(CmpType.Source, source.id)
      case sink: Conf.Sink ⇒ AddressRef(CmpType.Sink, sink.id)
      case _ ⇒ AddressRef(CmpType.Undefined, -1)
    }
  }
}

trait AddressFactory extends LazyLogging {
  var types = Map[String, Class[_]](
    "kafka_consumer" → classOf[KafkaConsumerAddressBuilder],
    "kafka_producer" → classOf[KafkaProducerAddressBuilder],
    "dataSource" → classOf[DataSourceAddressBuilder],
    "hbase.common"→classOf[HbaseAddressBuilder],
    "elasticsearch" → classOf[ElasticSearchAddressBuilder],
    "mongo" → classOf[MongoAddressBuilder],
    "influxdb" → classOf[InfluxdbAddressBuilder]
  )
  val addresses = TrieMap[Int, Address[_]]()
  val addressesBySource = TrieMap[Int, Address[_]]()
  val addressesRefs = new TrieMap[Int, mutable.Set[AddressRef]]

  def loadConf(id: Int): Conf.Address

  def addressing[T](sourceConf: Conf.Source)(implicit center: TeleporterCenter): T = {
    addressesBySource.get(sourceConf.id).asInstanceOf[Option[Address[T]]] match {
      case Some(address) ⇒ address.client
      case None ⇒
        val address = build(loadConf(sourceConf.addressId.get)).asInstanceOf[Address[T]]
        addressesBySource += (sourceConf.id → address)
        address.client
    }
  }

  /**
   * client will close where ref is empty
   */
  def addressing[T](id: Int, ref: Any)(implicit center: TeleporterCenter): T = getAddress[T](loadConf(id), ref).client

  private def getAddress[T](conf: Conf.Address, ref: Any)(implicit center: TeleporterCenter): Address[T] = {
    val id = conf.id
    addresses.get(id).asInstanceOf[Option[Address[T]]] match {
      case Some(address) ⇒ address
      case None ⇒
        val address = build(conf).asInstanceOf[Address[T]]
        this.synchronized {
          addressesRefs.getOrElseUpdate(id, mutable.Set[AddressRef]()) += AddressRef(ref)
        }
        addresses += (id → address)
        address
    }
  }

  private def build[T](conf: Conf.Address)(implicit center: TeleporterCenter): Address[T] = {
    val clazz = types(conf.category)
    val address = ConstructorUtils.invokeConstructor(clazz, conf, center)
    address.asInstanceOf[AddressBuilder[T]].build
  }

  def registerType[T <: Address[Any]](category: String, clazz: Class[T]): Unit = {
    types = types + (category → clazz)
  }

  def close(conf: Conf.Source): Unit = {
    val sourceId = conf.id
    conf.addressId.foreach(x ⇒ addresses.remove(x).foreach(_.close()))
    addressesBySource -= sourceId
  }

  def close(id: Int, ref: Any): Unit = {
    this.synchronized {
      addressesRefs.get(id).foreach {
        refs ⇒ if ((refs -= AddressRef(ref)).isEmpty) {
          addressesRefs -= id
          addresses.remove(id).foreach(_.close())
        }
      }
    }
  }
}

class AddressFactoryImpl(teleporterConfigFactory: TeleporterConfigFactory) extends AddressFactory {
  override def loadConf(id: Int): Conf.Address = teleporterConfigFactory.address(id)
}

object AddressFactory {
  def apply(configFactory: TeleporterConfigFactory): AddressFactory = new AddressFactoryImpl(configFactory)
}