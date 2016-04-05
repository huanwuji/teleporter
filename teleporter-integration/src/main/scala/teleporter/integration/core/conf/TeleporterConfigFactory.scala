package teleporter.integration.core.conf

import org.mongodb.scala.MongoDatabase
import teleporter.integration.conf.Conf
import teleporter.integration.conf.Conf.{Address, Sink, Source, Task}

import scala.concurrent.ExecutionContext

/**
 * date 2015/8/3.
 * @author daikui
 */
trait TeleporterConfigFactory {
  def task(id: Int): Conf.Task

  def address(id: Int): Conf.Address

  def source(id: Int): Conf.Source

  def sink(id: Int): Conf.Sink
}

class MongoTeleporterConfigFactory()(implicit db: MongoDatabase, ec: ExecutionContext) extends TeleporterConfigFactory {
  override def task(id: Int): Task = Task.get(id)

  override def address(id: Int): Address = Address.get(id)

  override def source(id: Int): Source = Source.get(id)

  override def sink(id: Int): Sink = Sink.get(id)
}

class LocalTeleporterConfigFactory(localStore: LocalTeleporterConfigStore) extends TeleporterConfigFactory {
  override def task(id: Int): Task = localStore.tasks(id)

  override def address(id: Int): Address = localStore.addresses(id)

  override def source(id: Int): Source = localStore.sources(id)

  override def sink(id: Int): Sink = localStore.sinks(id)
}

class LocalTeleporterConfigStore {
  var tasks = Map[Int, Conf.Task]()
  var addresses = Map[Int, Conf.Address]()
  var sources = Map[Int, Conf.Source]()
  var sinks = Map[Int, Conf.Sink]()
}