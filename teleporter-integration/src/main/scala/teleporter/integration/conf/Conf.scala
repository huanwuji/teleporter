package teleporter.integration.conf

import com.typesafe.scalalogging.LazyLogging
import org.mongodb.scala._
import teleporter.integration.component.Entity
import teleporter.integration.support.MongoSupport

import scala.concurrent.ExecutionContext

/**
 * Author: kui.dai
 * Date: 2015/11/16.
 */
object Conf {
  //json4s can't deserialize, so Conf not used
  type Props = Map[String, Any]

  val PROPS_EMPTY = Map.empty[String, Any]

  def props() = Map[String, Any]()

  trait PropsConf {
    val props: Props
  }

  object StreamStatus {
    val VALID = "VALID"
    val INVALID = "INVALID"
    val RUNNING = "RUNNING"
    val ERROR = "ERROR"
    val COMPLETE = "COMPLETE"
  }

  trait ConfBuilder[T <: Entity[Int]] extends MongoSupport[T, Int] with LazyLogging {
    def generateId(name: String, factor: Int = 1)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): Int = {
      val hashCode = Math.abs(generateHashCode(name, factor))
      getOption(hashCode) match {
        case Some(t) ⇒
          if (name == t.name) {
            logger.info(s"Will load exists id, $t")
            t.id
          } else {
            generateId(name, factor + 1)
          }
          t.id
        case None ⇒ hashCode
      }
    }

    private def generateHashCode(name: String, factor: Int = 1): Int = (1 to factor).map(_ ⇒ name).mkString("teleporter").hashCode
  }

  case class Task(id: Int = -1,
                  name: String,
                  desc: String,
                  props: Map[String, Any]) extends PropsConf with Entity[Int]

  object Task extends ConfBuilder[Task] {
    override val colName = "task"

    def safeSave(task: Task)(implicit client: MongoDatabase, ex: ExecutionContext): Task =
      if (task.id == -1) {
        val _task = task.copy(id = generateId(task.name))
        save(_task)
        _task
      } else {
        save(task)
        task
      }
  }

  case class Stream(id: Int = -1,
                    taskId: Int = -1,
                    name: String,
                    status: String = StreamStatus.VALID,
                    desc: String,
                    props: Map[String, Any] = Map()) extends PropsConf with Entity[Int]

  object Stream extends ConfBuilder[Stream] {
    override val colName = "stream"

    def safeSave(stream: Stream)(implicit client: MongoDatabase, ex: ExecutionContext): Stream =
      if (stream.id == -1) {
        val _stream = stream.copy(id = generateId(stream.name))
        save(_stream)
        _stream
      } else {
        save(stream)
        stream
      }
  }

  case class Address(id: Int = -1,
                     category: String,
                     name: String,
                     props: Map[String, Any]) extends PropsConf with Entity[Int]

  object Address extends ConfBuilder[Address] {
    override val colName = "address"

    def safeSave(address: Address)(implicit client: MongoDatabase, ex: ExecutionContext): Address =
      if (address.id == -1) {
        val _address = address.copy(id = generateId(address.name))
        save(_address)
        _address
      } else {
        save(address)
        address
      }
  }

  case class Source(id: Int = -1,
                    taskId: Int = -1,
                    streamId: Int = -1,
                    addressId: Option[Int] = None,
                    category: String,
                    name: String,
                    status: String = StreamStatus.RUNNING,
                    props: Map[String, Any] = Map()) extends PropsConf with Entity[Int]

  object Source extends ConfBuilder[Source] {
    override val colName = "source"

    def safeSave(source: Source)(implicit client: MongoDatabase, ex: ExecutionContext): Source =
      if (source.id == -1) {
        val _source = source.copy(id = generateId(source.name))
        save(_source)
        _source
      } else {
        save(source)
        source
      }
  }

  case class Sink(id: Int = -1,
                  taskId: Int = -1,
                  streamId: Int = -1,
                  addressId: Option[Int] = None,
                  category: String,
                  name: String,
                  props: Map[String, Any]) extends PropsConf with Entity[Int]

  object Sink extends ConfBuilder[Sink] {
    override val colName = "sink"

    def safeSave(sink: Sink)(implicit client: MongoDatabase, ex: ExecutionContext): Sink =
      if (sink.id == -1) {
        val _sink = sink.copy(id = generateId(sink.name))
        save(_sink)
        _sink
      } else {
        save(sink)
        sink
      }
  }

  case class GlobalProps(id: Int = -1, name: String, props: Map[String, Any]) extends PropsConf with Entity[Int]

  object GlobalProps extends ConfBuilder[GlobalProps] {
    override val colName = "global_props"

    def safeSave(globalProps: GlobalProps)(implicit client: MongoDatabase, ex: ExecutionContext): GlobalProps =
      if (globalProps.id == -1) {
        val _globalProps = globalProps.copy(id = generateId(globalProps.name))
        save(_globalProps)
        _globalProps
      } else {
        save(globalProps)
        globalProps
      }
  }

}