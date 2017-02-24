package teleporter.integration.component.kudu

import java.sql.{Date, Timestamp}

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.{Done, NotUsed}
import com.stumbleupon.async.Callback
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component._
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics
import teleporter.integration.utils.Bytes.toBytes

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by joker on 16/3/3
  */
object Kudu {
  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[KuduRecord], Message[KuduRecord], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val bind = Option(sinkContext.config.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new KuduSinkAsync(1,
      (ec) ⇒ Future {
        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
      }(ec), {
        (_, _) ⇒
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

trait KuduSupport {
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

case class KuduInsert(table: String, binds: Map[String, Any], version: Long = -1) extends KuduAction

case class KuduUpdate(table: String, binds: Map[String, Any], version: Long = -1) extends KuduAction

case class KuduUpsert(table: String, binds: Map[String, Any], version: Long = -1) extends KuduAction

object KuduMetaBean {
  val FKuduMaster = "kuduMaster"
  val FWorkerCount = "workerCount"
}

class KuduMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import KuduMetaBean._

  def kuduMaster: String = client[String](FKuduMaster)

  def workerCount: Int = client[Int](FWorkerCount)
}

class KuduSinkAsync(parallelism: Int = 1,
                    _create: (ExecutionContext) ⇒ Future[AsyncKuduClient],
                    _close: (AsyncKuduClient, ExecutionContext) ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[AsyncKuduClient, Message[KuduRecord], Message[KuduRecord]]("kafka.sink", parallelism) with KuduSupport with Logging {
  val tables: mutable.Map[String, KuduTable] = mutable.Map[String, KuduTable]()
  var session: AsyncKuduSession = _

  override def create(executionContext: ExecutionContext): Future[AsyncKuduClient] = {
    implicit val ec = executionContext
    _create(executionContext).map { _client ⇒
      session = _client.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
      _client
    }
  }

  override def write(client: AsyncKuduClient, elem: Message[KuduRecord], executionContext: ExecutionContext): Future[Message[KuduRecord]] = {
    val promise = Promise[Message[KuduRecord]]()
    val data = elem.data
    val tableName = data.table
    val table = tables.getOrElseUpdate(tableName, client.openTable(tableName).join(5000))
    val operation = elem match {
      case _: KuduInsert ⇒ table.newInsert()
      case _: KuduUpdate ⇒ table.newUpdate()
      case _: KuduUpsert ⇒ table.newUpsert()
    }
    session.apply(addRows(operation, data.binds))
      .addCallbacks(new Callback[Object, OperationResponse] {
        override def call(arg: OperationResponse): Object = {
          if (arg.getRowError.getErrorStatus.ok()) {
            promise.success(elem)
          } else {
            logger.warn(arg.getRowError.toString)
            null
          }
        }
      }, new Callback[Object, Exception] {
        override def call(arg: Exception): Object = {
          promise.failure(arg)
        }
      })
    promise.future
  }

  override def close(client: AsyncKuduClient, executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}