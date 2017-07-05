package teleporter.integration.component.kudu

import java.sql.{Date, Timestamp}
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.{Done, NotUsed}
import com.stumbleupon.async.Callback
import org.apache.kudu.Common.DataType
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component._
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics
import teleporter.integration.utils.Bytes.toBytes
import teleporter.integration.utils.Converters._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by joker on 16/3/3
  */
object Kudu {
  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[KuduRecord], Message[KuduRecord], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[KuduSinkMetaBean]
    val bind = Option(sinkContext.config.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new KuduSinkAsync(
      name = sinkKey,
      parallelism = sinkConfig.parallelism,
      autoFit = sinkConfig.autoFit,
      _create = (ec) ⇒ Future {
        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
      }(ec),
      _close = (_, _) ⇒ {
        center.context.unRegister(addressKey, bind)
        Future.successful(Done)
      }))
      .addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
      .via(Metrics.count[Message[KuduRecord]](sinkKey)(center.metricsRegistry))
  }

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[KuduRecord], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def address(addressKey: String)(implicit center: TeleporterCenter): AutoCloseClientRef[AsyncKuduClient] = {
    val addressContext = center.context.getContext[AddressContext](addressKey)
    val kuduMetaBean = addressContext.config.mapTo[KuduMetaBean]
    val asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMetaBean.kuduMaster)
      .workerCount(kuduMetaBean.workerCount).build()
    AutoCloseClientRef(addressKey, asyncClient)
  }
}

object KuduSinkMetaBean {
  val FParallelism = "parallelism"
  val FAutoFit = "autoFit"
}

class KuduSinkMetaBean(override val underlying: Map[String, Any]) extends SinkMetaBean(underlying) {

  import KuduSinkMetaBean._

  def parallelism: Int = client.get[Int](FParallelism).getOrElse(1)

  def autoFit: String = client.get[String](FAutoFit).getOrElse("DEFAULT")
}

trait KuduSupport {
  def addRowsBySchema(operation: Operation, binds: Map[String, Any], table: KuduTable): Operation = {
    val row = operation.getRow
    table.getSchema.getColumns.foreach { column ⇒
      binds.get(column.getName) match {
        case Some(value) ⇒
          column.getType.getDataType match {
            case DataType.STRING ⇒
              addValueOrNull[String](asNonEmptyString(value), row.addString(column.getName, _), row.setNull(column.getName))
            case DataType.INT16 ⇒
              addValueOrNull[Short](asShort(value), row.addShort(column.getName, _), row.setNull(column.getName))
            case DataType.INT32 ⇒
              addValueOrNull[Int](asInt(value), row.addInt(column.getName, _), row.setNull(column.getName))
            case DataType.INT64 ⇒
              addValueOrNull[Long](asLong(value), row.addLong(column.getName, _), row.setNull(column.getName))
            case DataType.DOUBLE ⇒
              addValueOrNull[Double](asDouble(value), row.addDouble(column.getName, _), row.setNull(column.getName))
            case DataType.FLOAT ⇒
              addValueOrNull[Float](asFloat(value), row.addFloat(column.getName, _), row.setNull(column.getName))
            case DataType.BOOL ⇒
              addValueOrNull[Boolean](asBoolean(value), row.addBoolean(column.getName, _), row.setNull(column.getName))
            case DataType.BINARY ⇒
              addValueOrNull[Array[Byte]](asBytes(value), row.addBinary(column.getName, _), row.setNull(column.getName))
          }
        case None ⇒ row.setNull(column.getName)
      }
    }
    operation
  }

  protected def addValueOrNull[T](value: Option[T], add: T ⇒ Unit, default: ⇒ Unit): Unit = value match {
    case Some(v) ⇒ add(v)
    case None ⇒ default
  }

  def addRows(operation: Operation, binds: Map[String, Any]): Operation = {
    val row = operation.getRow
    binds.foreach {
      case (name, value) ⇒
        value match {
          case v: Boolean ⇒ row.addBoolean(name, v)
          case v: Short ⇒ row.addShort(name, v)
          case v: Int ⇒ row.addInt(name, v)
          case v: Long ⇒ row.addLong(name, v)
          case v: Float ⇒ row.addFloat(name, v)
          case v: Double ⇒ row.addDouble(name, v)
          case v: BigDecimal ⇒ row.addBinary(name, toBytes(v))
          case v: java.math.BigDecimal ⇒ row.addBinary(name, toBytes(v))
          case v: Date ⇒ row.addLong(name, v.getTime)
          case v: Timestamp ⇒ row.addLong(name, v.getTime)
          case v: String ⇒ row.addString(name, v)
          case v: Array[Byte] ⇒ row.addBinary(name, v)
          case null ⇒ row.setNull(name)
          case v ⇒ throw new IllegalArgumentException(s"Unsupport data type $v, ${v.getClass.getSimpleName}")
        }
    }
    operation
  }
}

sealed trait KuduAction {
  val table: String
  val binds: Map[String, Any]
  val version: Long
}

case class KuduInsert(table: String, binds: Map[String, Any] = Map.empty, version: Long = -1) extends KuduAction

case class KuduUpdate(table: String, binds: Map[String, Any] = Map.empty, version: Long = -1) extends KuduAction

case class KuduUpsert(table: String, binds: Map[String, Any] = Map.empty, version: Long = -1) extends KuduAction

class KuduMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {
  val FKuduMaster = "kuduMaster"
  val FWorkerCount = "workerCount"

  def kuduMaster: String = client[String](FKuduMaster)

  def workerCount: Int = client[Int](FWorkerCount)
}

class KuduSinkAsync(
                     name: String = "kudu.sink",
                     parallelism: Int,
                     autoFit: String,
                     _create: (ExecutionContext) ⇒ Future[AsyncKuduClient],
                     _close: (AsyncKuduClient, ExecutionContext) ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[AsyncKuduClient, Message[KuduRecord], Message[KuduRecord]](name, parallelism, identity) with KuduSupport with Logging {
  val tables: mutable.Map[String, KuduTable] = mutable.Map[String, KuduTable]()
  var session: AsyncKuduSession = _

  val asyncParallelism = 6

  private def useSync = parallelism < asyncParallelism

  override def create(executionContext: ExecutionContext): Future[AsyncKuduClient] = {
    implicit val ec = executionContext
    _create(executionContext).map { _client ⇒
      session = _client.newSession()
      session.setFlushMode(if (useSync) FlushMode.AUTO_FLUSH_SYNC else FlushMode.MANUAL_FLUSH)
      _client
    }
  }

  private val count = new AtomicInteger()

  override def write(client: AsyncKuduClient, elem: Message[KuduRecord], executionContext: ExecutionContext): Future[Message[KuduRecord]] = {
    count.incrementAndGet()
    val promise = Promise[Message[KuduRecord]]()
    val data = elem.data
    val tableName = data.table
    val table = tables.getOrElseUpdate(tableName, client.openTable(tableName).join(10000))
    val operation = elem.data match {
      case _: KuduInsert ⇒ table.newInsert()
      case _: KuduUpdate ⇒ table.newUpdate()
      case _: KuduUpsert ⇒ table.newUpsert()
    }
    val fillOperation = autoFit match {
      case "DEFAULT" ⇒ addRows(operation, data.binds)
      case "SCHEMA" ⇒ addRowsBySchema(operation, data.binds, table)
    }
    session.apply(fillOperation)
      .addCallbacks(new Callback[Object, OperationResponse] {
        override def call(arg: OperationResponse): Object = {
          promise.success(elem)
          count.decrementAndGet()
          if (arg.getRowError.getErrorStatus.ok()) {
          } else {
            logger.warn(arg.getRowError.toString)
          }
          null
        }
      }, new Callback[Object, Exception] {
        override def call(arg: Exception): Object = {
          promise.failure(arg)
          count.decrementAndGet()
          null
        }
      })
    if (!useSync && count.get() > parallelism / 3) {
      session.flush()
    }
    promise.future
  }

  override def close(client: AsyncKuduClient, executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}