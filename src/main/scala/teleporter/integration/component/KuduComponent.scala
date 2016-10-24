package teleporter.integration.component

import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, RequestStrategy}
import com.typesafe.scalalogging.LazyLogging
import org.kududb.Common.DataType
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.client._
import teleporter.integration.ClientApply
import teleporter.integration.core._
import teleporter.integration.support.RocksdbSupport
import teleporter.integration.utils.Converters._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by joker on 16/3/3
  */
case class KuduDataSource(client: KuduClient, session: KuduSession, table: KuduTable) extends AutoCloseable {
  def close(): Unit = {
    if (!session.isClosed) {
      session.close()
    }
    client.close()
  }
}

class KuduSubscriber(override val key: String)(implicit val center: TeleporterCenter, implicit val rocksdb: RocksdbSupport[TId])
  extends ActorSubscriber with Component with LazyLogging {
  val sinkContext = center.context.getContext[SinkContext](key)
  val client = center.components.address[KuduDataSource](key)
  var columnMap = new mutable.HashMap[String, DataType]()
  var flag = false

  override protected def requestStrategy: RequestStrategy = RequestStrategyManager(true)

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    client.table.getSchema.getColumns.asScala.map(
      col ⇒ {
        columnMap += col.getName → col.getType.getDataType
      }
    )
  }

  override def receive: Receive = {
    case OnNext(kuduAction: KuduAction) ⇒
      try {
        delivery(kuduAction)
        rocksdb.deleteByKey(kuduAction.message.id)
      }
      catch {
        case ex: Exception ⇒ context.become(retry); self ! kuduAction
      }
    case OnComplete ⇒
      center.context.getContext[AddressContext](sinkContext.addressKey).clientRefs.close(key)
    case OnError(e) ⇒
      center.context.getContext[AddressContext](sinkContext.addressKey).clientRefs.close(key)
      logger.error(e.getLocalizedMessage, e)
  }


  @throws[Exception](classOf[Exception])
  def delivery(kuduAction: KuduAction): Unit = {
    kuduAction match {
      case action: KuduInsert ⇒ process(client.table.newInsert(), action.message)
      case action: KuduUpdate ⇒ process(client.table.newUpdate(), action.message)
    }
  }

  def process(operation: Operation, element: TeleporterJdbcMessage): Unit = {
    val row = operation.getRow
    element.data.foreach(
      entry ⇒ {
        if (columnMap.contains(entry._1)) {
          val colType = columnMap(entry._1)
          colType match {
            case DataType.DOUBLE ⇒ row.addDouble(entry._1, entry._2.toString.toDouble)
            case DataType.STRING ⇒ row.addString(entry._1, entry._2.toString)
            case DataType.INT64 ⇒ row.addLong(entry._1, entry._2.toString.toLong)
            case DataType.INT32 ⇒ row.addInt(entry._1, entry._2.toString.toInt)
            case DataType.BOOL ⇒ row.addBoolean(entry._1, entry._2.toString.toBoolean)
          }
        }
      })
    client.session.apply(operation)

  }

  def retry: Receive = {
    case kuduAction: KuduAction ⇒
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
  }
}

trait KuduAction {
  val message: TeleporterJdbcMessage
}

case class KuduInsert(override val message: TeleporterJdbcMessage) extends KuduAction

case class KuduUpdate(override val message: TeleporterJdbcMessage) extends KuduAction

trait KuduMetadata extends AddressMetadata {
  val KUDU_MASTER = "kudu_master"
  val KUDU_TABLE_NAME = "kudu_table_name"
}

object KuduComponent extends KuduMetadata {
  val kuduClient: ClientApply = (key, center) ⇒ {
    val addressContext = center.context.getContext[AddressContext](key)
    val clientConfig = lnsClient(addressContext.config)
    val client: KuduClient = new KuduClient.KuduClientBuilder(clientConfig[String](KUDU_MASTER).toString).build
    val kuduTable: KuduTable = client.openTable(KUDU_TABLE_NAME)
    val session = client.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_SYNC)
    AutoCloseClientRef(key, KuduDataSource(client, session, kuduTable))
  }
}
