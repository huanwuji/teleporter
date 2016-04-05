package teleporter.integration.core.conf

import org.mongodb.scala.MongoDatabase
import teleporter.integration.CmpType
import teleporter.integration.conf.Conf
import teleporter.integration.conf.Conf._

import scala.concurrent.ExecutionContext

/**
 * date 2015/8/3.
 * @author daikui
 */
trait NameMapper {
  val nameMap = scala.collection.mutable.Map[(String,CmpType),Int]()
  val configFactory: TeleporterConfigFactory

  def getId(name: String, cmpType: CmpType): Int = {
    val key = (name, cmpType)
    nameMap.getOrElseUpdate(key, cmpType match {
      case CmpType.Task ⇒ configFactory.task(name).id
      case CmpType.Stream ⇒ configFactory.stream(name).id
      case CmpType.Address ⇒ configFactory.address(name).id
      case CmpType.Source ⇒ configFactory.source(name).id
      case CmpType.Sink ⇒ configFactory.sink(name).id
      case CmpType.Shadow ⇒ configFactory.source(name).id
      case _ ⇒ -1
    })
  }
}

class NameMapperImpl(val configFactory: TeleporterConfigFactory) extends NameMapper

object NameMapper {
  def apply(configFactory: TeleporterConfigFactory) = new NameMapperImpl(configFactory)
}

trait TeleporterConfigFactory {
  val nameMapper = NameMapper(this)

  def task(id: Int): Conf.Task

  def stream(id: Int): Conf.Stream

  def stream(name: String): Conf.Stream

  def streams(taskId: Int): Seq[Conf.Stream]

  def address(id: Int): Conf.Address

  def source(id: Int): Conf.Source

  def sink(id: Int): Conf.Sink

  def globalProps(id: Int): Conf.GlobalProps

  def task(name: String): Conf.Task

  def address(name: String): Conf.Address

  def source(name: String): Conf.Source

  def sink(name: String): Conf.Sink

  def globalProps(name: String): Conf.GlobalProps

  def tasks(matches: String): Seq[Conf.Task]

  def addresses(matches: String): Seq[Conf.Address]

  def sources(matches: String): Seq[Conf.Source]

  def sinks(matches: String): Seq[Conf.Sink]

  def globalsProps(matches: String): Seq[Conf.GlobalProps]

  def taskSources(taskId: Int): Seq[Conf.Source] = ???

  def taskSinks(taskId: Int): Seq[Conf.Sink] = ???

  def save(task: Task): Task

  def save(address: Address): Address

  def save(source: Source): Source

  def save(sink: Sink): Sink

  def save(globalProps: GlobalProps): GlobalProps
}

class MongoTeleporterConfigFactory()(implicit db: MongoDatabase, ec: ExecutionContext) extends TeleporterConfigFactory {
  override def task(id: Int): Task = Task.get(id)

  def stream(id: Int): Conf.Stream = Stream.get(id)

  def stream(name: String): Conf.Stream = Stream.findByName(name)

  override def streams(taskId: Int): Seq[Conf.Stream] = Stream.findByTask(taskId)

  override def address(id: Int): Address = Address.get(id)

  override def source(id: Int): Source = Source.get(id)

  override def sink(id: Int): Sink = Sink.get(id)

  override def task(name: String): Conf.Task = Task.findByName(name)

  override def address(name: String): Conf.Address = Address.findByName(name)

  override def source(name: String): Conf.Source = Source.findByName(name)

  override def sink(name: String): Conf.Sink = Sink.findByName(name)

  override def taskSources(taskId: Int): Seq[Conf.Source] = Source.findByTask(taskId)

  override def taskSinks(taskId: Int): Seq[Conf.Sink] = Sink.findByTask(taskId)

  override def save(task: Task): Task = Task.safeSave(task)

  override def save(address: Address): Address = Address.safeSave(address)

  override def save(source: Source): Source = Source.safeSave(source)

  override def save(sink: Sink): Sink = Sink.safeSave(sink)

  override def save(globalProps: GlobalProps): GlobalProps = GlobalProps.safeSave(globalProps)

  override def globalProps(id: Int): GlobalProps = GlobalProps.get(id)

  override def globalProps(name: String): GlobalProps = GlobalProps.findByName(name)

  override def tasks(matches: String): Seq[Task] = Task.matchName(matches)

  override def sinks(matches: String): Seq[Sink] = Sink.matchName(matches)

  override def sources(matches: String): Seq[Source] = Source.matchName(matches)

  override def addresses(matches: String): Seq[Address] = Address.matchName(matches)

  def globalsProps(matches: String): Seq[Conf.GlobalProps] = GlobalProps.matchName(matches)
}