package teleporter.integration.cluster.broker

import teleporter.integration.cluster.broker.PersistentProtocol.{KeyValue, Keys}
import teleporter.integration.core.TeleporterConfig
import teleporter.integration.utils.Jackson

import scala.reflect._

/**
  * Created by kui.dai on 2016/7/15.
  */
trait PersistentService {

  def id(): Long

  def range(key: String, start: Int = 0, limit: Int = Int.MaxValue): Seq[KeyValue]

  //prefix regex match
  def regexRange(tailRegex: String, start: Int = 0, limit: Int = Int.MaxValue): Seq[KeyValue] = {
    Keys.tailRegexSplit.findFirstMatchIn(tailRegex) match {
      case Some(m) ⇒
        val prefixKey = tailRegex.substring(0, m.start)
        val regex = tailRegex.substring(m.start).r
        this.range(prefixKey, start, limit)
          .filter(kv ⇒ regex.findPrefixOf(kv.key.substring(m.start)).isDefined)
      case None ⇒ this.range(tailRegex, start, limit)
    }
  }

  def apply(key: String): KeyValue

  def get(key: String): Option[KeyValue]

  def put(key: String, value: String): Unit

  def delete(key: String): Unit

  def atomicPut(key: String, expect: String, update: String): Boolean
}

object PersistentProtocol {

  trait Key {
    val key: String

    def unapplyKey(template: String): Map[String, String] = Keys.unapply(this.key, template)

    def section(sectionName: String, template: String): String = unapplyKey(template)(sectionName)
  }

  object Tables {
    val group = "group"
    val broker = "broker"
    val instance = "instance"
    val namespace = "namespace"
    val task = "task"
    val stream = "stream"
    val partition = "partition"
    val source = "source"
    val sink = "sink"
    val address = "address"
    val variable = "variable"
  }

  object Keys {
    val delimiter = ":(\\w+)".r

    //{tasks:[], instances:[]}
    val GROUP = "/group/:ns/:group"
    //{ip:"",port:""}
    val BROKER = "/broker/:ns/:broker"
    val BROKERS = "/broker/:ns/"
    //{tasks:[:task], group:""}
    val INSTANCE = "/instance/:ns/:instance"
    val NAMESPACE = "/namespace/:namespace"
    //{instances:[:instance], group: ""}
    val TASK = "/task/:ns/:task"
    //{keys:["/stream/:ns/:task/:streamRegex"]}
    val PARTITION = "/partition/:ns/:task/:partition"
    val PARTITIONS = "/partition/:ns/:task/"
    val STREAM = "/stream/:ns/:task/:stream"
    val STREAMS = "/stream/:ns/:task/"
    val SOURCE = "/source/:ns/:task/:stream/:source"
    val TASK_SOURCES = "/source/:ns/:task/"
    val STREAM_SOURCES = "/source/:ns/:task/:stream/"
    val SINK = "/sink/:ns/:task/:stream/:sink"
    val TASK_SINKS = "/sink/:ns/:task/"
    val STREAM_SINKS = "/sink/:ns/:task/:stream/"
    val ADDRESS = "/address/:ns/:address"
    val VARIABLE = "/variable/:ns/:variable"
    //{keys:[], timestamp:194893439434}
    val RUNTIME_ADDRESSES = "/address/:ns/:address/links/"
    val RUNTIME_ADDRESS = "/address/:ns/:address/links/:instance"
    //{keys:[], timestamp:13489348934}
    val RUNTIME_VARIABLES = "/variable/:ns/:variable/links/"
    val RUNTIME_VARIABLE = "/variable/:ns/:variable/links/:instance"
    // value: {instance:"/",timestamp:139489343948}
    val RUNTIME_PARTITION = "/partition/:ns/:task/:partition"
    val RUNTIME_PARTITIONS = "/partition/:ns/:task/"
    //{timestamp:13439483434}
    val RUNTIME_TASK_BROKER = "/task/:ns/:task/broker/:broker"
    //{timestamp:13439483434}
    val RUNTIME_BROKER_TASK = "/broker/:broker/task/:ns/:task"
    //{timestamp:13439483434}
    val RUNTIME_BROKER = "/broker/:broker"
    //{ip:"", port:1133}
    val RUNTIME_INSTANCE = "/instance/:ns/:instance"

    def apply(template: String, args: String*): String = {
      val it = args.toIterator
      delimiter.replaceAllIn(template, it.next())
    }

    def apply(template: String, params: Map[String, String]): String = {
      delimiter.replaceAllIn(template, m ⇒ params(m.group(1)))
    }

    def mapping(key: String, templateSource: String, templateTarget: String): String = {
      apply(templateTarget, unapply(key, templateSource))
    }

    def unapply(key: String, template: String): Map[String, String] = {
      template.split("/").zip(key.split("/")).filter(_._1.startsWith(":"))
        .map { case (v1, v2) ⇒ (v1.tail, v2) }.toMap
    }

    def table(key: String): String = {
      unapply(key, "/:table")("table")
    }

    val tailRegexSplit = "[^\\w/]".r

    def belongRegex(tailRegex: String, key: String): Boolean = {
      tailRegexSplit.findFirstMatchIn(tailRegex) match {
        case Some(m) ⇒
          val prefixKey = tailRegex.substring(0, m.start)
          val regex = tailRegex.substring(m.start).r
          key.substring(0, m.start) == prefixKey && regex.findPrefixOf(key.substring(m.start)).isDefined
        case None ⇒ tailRegex == key
      }
    }
  }

  object Values {

    trait Value {
      def toJson = Jackson.mapper.writeValueAsString(this)
    }

    object InstanceStatus {
      val offline = "OFFLINE"
      val online = "ONLINE"
    }

    case class RuntimeAddressValue(keys: Set[String], timestamp: Long) extends Value

    case class RuntimeVariableValue(keys: Set[String], timestamp: Long) extends Value

    case class RuntimeInstanceValue(ip: String, port: Int, status: String, broker: String, timestamp: Long) extends Value

    case class RuntimePartitionValue(instance: String, status: String, timestamp: Long) extends Value

    case class GroupValue(tasks: Set[String], instances: Set[String], instanceOfflineReBalanceTime: String)

    case class BrokerValue(ip: String, port: Int, tcpPort: Int)

    case class InstanceValue(id: Long, group: String) extends Value

    case class TaskValue(instances: Set[String], group: String)

    case class PartitionValue(id: Long, keys: Set[String]) extends Value

    case class Version(timestamp: Long = System.currentTimeMillis()) extends Value

  }

  case class KeyBean[T](key: String, value: T) extends Key

  case class KeyValue(key: String, value: String) extends Key {
    def keyBean[T: Manifest]: KeyBean[T] = KeyBean(key, Jackson.mapper.readValue[T](value))

    def config: KeyBean[TeleporterConfig] = KeyBean(key, TeleporterConfig(Jackson.mapper.readValue[Map[String, Any]](value)))
  }

  case class AtomicKeyValue(key: String, expect: String, update: String)

}