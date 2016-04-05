package teleporter.integration.core

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.reflect.ConstructorUtils
import teleporter.integration.component.hbase.HbaseAddressBuilder
import teleporter.integration.component.jdbc.DataSourceAddressBuilder
import teleporter.integration.component.{ElasticSearchAddressBuilder, KafkaConsumerAddressBuilder, KafkaProducerAddressBuilder}
import teleporter.integration.conf.Conf
import teleporter.integration.core.conf.TeleporterConfigFactory

/**
 * date 2015/8/3.
 * @author daikui
 */
trait AddressBuilder[A] {
  def teleporterCenter: TeleporterCenter

  def conf: Conf.Address

  def build: Address[A]
}

trait Address[A] {
  val _conf: Conf.Address
  val client: A

  def close(): Unit = {}
}

trait AddressFactory extends LazyLogging {
  implicit val teleporterCenter: TeleporterCenter
  var types = Map[String, Class[_]](
    "kafka_consumer" → classOf[KafkaConsumerAddressBuilder],
    "kafka_producer" → classOf[KafkaProducerAddressBuilder],
    "dataSource" → classOf[DataSourceAddressBuilder],
    "hbase.common"->classOf[HbaseAddressBuilder],
    "elasticsearch" -> classOf[ElasticSearchAddressBuilder]
  )
  var addresses = Map[Int, Address[_]]()
  var addressesBySource = Map[Int, Address[_]]()

  def loadConf(id: Int): Conf.Address

  def addressing[T](sourceConf: Conf.Source): Option[T] = {
    val conf = loadConf(sourceConf.addressId.get)
    val addressOpt = addressesBySource.get(sourceConf.id)
    if (addressOpt.isEmpty) {
      val address = build(conf).asInstanceOf[Address[T]]
      addressesBySource = addressesBySource + (sourceConf.id → address)
      Some(address.client)
    } else {
      addressOpt.asInstanceOf[Option[Address[T]]].map(_.client)
    }
  }

  def addressing[T](id: Int): Option[T] = {
    val conf = loadConf(id)
    val addressOpt = addresses.get(id)
    if (addressOpt.isEmpty) {
      val address = build(conf).asInstanceOf[Address[T]]
      addresses = addresses + (id → address)
      Some(address.client)
    } else {
      addressOpt.asInstanceOf[Option[Address[T]]].map(_.client)
    }
  }

  private def build[T](conf: Conf.Address): Address[T] = {
    val clazz = types(conf.category)
    val address = ConstructorUtils.invokeConstructor(clazz, conf, teleporterCenter)
    address.asInstanceOf[AddressBuilder[T]].build
  }

  def registerType[T <: Address[Any]](addressName: String, clazz: Class[T]): Unit = {
    types = types + (addressName → clazz)
  }

  def close(conf: Conf.Source): Unit = {
    val sourceId = conf.id
    addressesBySource(sourceId).close()
    addressesBySource = addressesBySource - sourceId
  }

  def close(id: Int): Unit = {
    addresses(id).close()
    addresses = addresses - id
  }
}

object AddressFactory {
  def apply(teleporterConfigFactory: TeleporterConfigFactory)(implicit _teleporterCenter: TeleporterCenter, _system: ActorSystem): AddressFactory = {
    new AddressFactory {
      override implicit val teleporterCenter: TeleporterCenter = _teleporterCenter

      override def loadConf(id: Int): Conf.Address = teleporterConfigFactory.address(id)

    }
  }
}