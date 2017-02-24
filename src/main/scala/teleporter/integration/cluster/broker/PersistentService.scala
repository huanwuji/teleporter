package teleporter.integration.cluster.broker

import com.typesafe.config.Config
import org.mongodb.scala.MongoClient
import teleporter.integration.cluster.broker.PersistentProtocol.{KeyValue, Keys}
import teleporter.integration.cluster.broker.leveldb.LevelDBService
import teleporter.integration.cluster.broker.mongo.MongoDBService
import teleporter.integration.cluster.broker.rocksdb.RocksDBService
import teleporter.integration.component.kv.leveldb.LevelDBs
import teleporter.integration.component.kv.rocksdb.RocksDBs
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{Jackson, MapBean, MapMetaBean}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.matching.Regex

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

  def put(key: String, value: String): KeyValue = {
    val map = Jackson.mapper.readValue[Map[String, Any]](value)
    val newValue = map.get("id").flatMap(asNonEmptyString) match {
      case Some(_) ⇒ Jackson.mapper.writeValueAsString(map)
      case _ ⇒
        val newMap = this.synchronized {
          get(key) match {
            case Some(kv) ⇒
              val oldMap = Jackson.mapper.readValue[Map[String, Any]](kv.value)
              oldMap.get("id") match {
                case Some(id) if id != null && asString(id).nonEmpty ⇒ map + ("id" → id)
                case _ ⇒ map + ("id" → id())
              }
            case None ⇒ map + ("id" → id())
          }
        }
        Jackson.mapper.writeValueAsString(newMap)
    }
    unsafePut(key, newValue)
    KeyValue(key, newValue)
  }

  protected def unsafePut(key: String, value: String): Unit

  def delete(key: String): Unit

  def atomicPut(key: String, expect: String, update: String): Boolean = {
    unsafeAtomicPut(key, reorder(expect), reorder(update))
  }

  protected def unsafeAtomicPut(key: String, expect: String, update: String): Boolean

  def reorder(value: String): String = {
    val map = Jackson.mapper.readValue[Map[String, Any]](value)
    Jackson.mapper.writeValueAsString(map)
  }
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
    val delimiter: Regex = ":(\\w+)".r

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
    val FLOW = "/:flow/:ns/:task/:stream/"
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

    def flowToStream(key: String): String = mapping(key, FLOW, STREAM)

    def flowToTask(key: String): String = mapping(key, FLOW, TASK)

    def table(key: String): String = {
      unapply(key, "/:table")("table")
    }

    val tailRegexSplit: Regex = "[^\\w/]".r

    def belongRegex(tailRegex: String, key: String): Boolean = {
      tailRegexSplit.findFirstMatchIn(tailRegex) match {
        case Some(m) ⇒
          val prefixKey = tailRegex.substring(0, m.start)
          val regex = tailRegex.substring(m.start).r
          key.substring(0, m.start) == prefixKey && regex.findPrefixOf(key.substring(m.start)).isDefined
        case None ⇒ key.startsWith(tailRegex)
      }
    }
  }

  object Values {

    trait Value {
      def toJson: String = Jackson.mapper.writeValueAsString(this)
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

    case class TaskValue(group: String)

    case class PartitionValue(id: Long, keys: Set[String]) extends Value

    case class Version(timestamp: Long = System.currentTimeMillis()) extends Value

  }

  case class KeyBean[T](key: String, value: T) extends Key

  case class KeyValue(key: String, value: String) extends Key {
    def keyBean[T: Manifest]: KeyBean[T] = KeyBean(key, Jackson.mapper.readValue[T](value))

    def mapBean: KeyBean[MapBean] = KeyBean(key, MapBean(Jackson.mapper.readValue[Map[String, Any]](value)))

    def metaBean[T <: MapBean : ClassTag] = KeyBean(key, MapMetaBean[T](Jackson.mapper.readValue[Map[String, Any]](value)))
  }

  case class AtomicKeyValue(key: String, expect: String, update: String)

}

object PersistentService {
  def apply(config: Config)(implicit ec: ExecutionContext): (PersistentService, PersistentService) = {
    val storageConfig = config.getConfig("storage")
    storageConfig.getString("type") match {
      case "leveldb" ⇒
        (LevelDBService(LevelDBs.applyTable("teleporter", "/config")), LevelDBService(LevelDBs.applyTable("teleporter", "/runtime")))
      case "rocksdb" ⇒
        (RocksDBService(RocksDBs.applyTable("teleporter", "/config")), RocksDBService(RocksDBs.applyTable("teleporter", "/runtime")))
      case "mongo" ⇒
        val mongoConfig = storageConfig.getConfig("mongo")
        val mongoClient = MongoClient(mongoConfig.getString("uri"))
        val database = mongoClient.getDatabase(mongoConfig.getString("database"))
        (MongoDBService(database.getCollection("config")), MongoDBService(database.getCollection("runtime")))
    }
  }
}