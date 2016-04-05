package teleporter.integration.conf

import teleporter.integration.component.IdEntity
import teleporter.integration.support.MongoSupport

/**
 * Author: kui.dai
 * Date: 2015/11/16.
 */
object Conf {
  //json4s can't deserialize, so Conf not used
  type Props = Map[String, Any]

  trait PropsConf {
    val props: Props
  }

  def generateId(name: String): Int = math.abs(name.hashCode)

  object SourceStatus {
    val INVALID = "INVALID"
    val NORMAL = "NORMAL"
    val ERROR = "ERROR"
    val COMPLETE = "COMPLETE"
  }

  case class Task(id: Int,
                  name: String,
                  desc: String,
                  props: Map[String, Any]) extends PropsConf with IdEntity[Int]

  object Task extends MongoSupport[Task, Int] {
    override val colName = "task"

    def apply(name: String, desc: String, props: Props): Task = {
      Task(id = generateId(name),
        name = name,
        desc = desc,
        props = props)
    }
  }

  case class Address(id: Int,
                     taskId: Option[Int] = None,
                     category: String,
                     name: String,
                     props: Map[String, Any]) extends PropsConf with IdEntity[Int]

  object Address extends MongoSupport[Address, Int] {
    override val colName = "address"

    def apply(taskId: Option[Int], category: String, name: String, props: Props): Address = {
      Address(id = generateId(name),
        taskId = taskId,
        category = category,
        name = name,
        props = props)
    }
  }

  case class Source(id: Int,
                    taskId: Option[Int] = None,
                    addressId: Option[Int] = None,
                    category: String,
                    name: String,
                    status: String = SourceStatus.NORMAL,
                    props: Map[String, Any]) extends PropsConf with IdEntity[Int]

  object Source extends MongoSupport[Source, Int] {
    override val colName = "source"

    def apply(taskId: Option[Int], addressId: Option[Int], category: String, name: String, props: Props): Source = {
      Source(id = generateId(name),
        taskId = taskId,
        addressId = addressId,
        category = category,
        name = name,
        status = null,
        props = props)
    }
  }

  case class Sink(id: Int,
                  taskId: Option[Int] = None,
                  addressId: Option[Int] = None,
                  category: String,
                  name: String,
                  props: Map[String, Any]) extends PropsConf with IdEntity[Int]

  object Sink extends MongoSupport[Sink, Int] {
    override val colName = "sink"

    def apply(taskId: Option[Int], addressId: Option[Int], category: String, name: String, props: Props): Sink = {
      Sink(id = generateId(name),
        taskId = taskId,
        addressId = addressId,
        category = category,
        name = name,
        props = props)
    }
  }

}