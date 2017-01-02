package teleporter.integration.component.kudu

import java.sql.{Date, Timestamp}

import akka.actor.Props
import akka.stream.actor.ActorSubscriberMessage.OnNext
import com.stumbleupon.async.Callback
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import teleporter.integration.ClientApply
import teleporter.integration.component._
import teleporter.integration.core._
import teleporter.integration.utils.Bytes.toBytes

import scala.collection.mutable

/**
  * Created by joker on 16/3/3
  */
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

class KuduSubscriberWork(val client: AsyncKuduClient) extends SubscriberWorker[AsyncKuduClient] with KuduSupport {
  val session: AsyncKuduSession = client.newSession()
  session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
  val tables: mutable.Map[String, KuduTable] = mutable.Map[String, KuduTable]()

  override def handle(onNext: OnNext, nrOfRetries: Int): Unit = {
    onNext.element match {
      case message: TeleporterKuduRecord ⇒
        val tableName = message.data.table
        val table = tables.getOrElseUpdate(tableName, client.openTable(tableName).join(5000))
        val operation = message.data match {
          case _: KuduInsert ⇒ table.newInsert()
          case _: KuduUpdate ⇒ table.newUpdate()
          case _: KuduUpsert ⇒ table.newUpsert()
        }
        session.apply(addRows(operation, message.data.binds))
          .addCallbacks(new Callback[Object, OperationResponse] {
            override def call(arg: OperationResponse): Object = {
              if (arg.getRowError.getErrorStatus.ok()) {
                success(onNext)
              }
              null
            }
          }, new Callback[Object, Exception] {
            override def call(arg: Exception): Object = {
              failure(onNext, arg, nrOfRetries)
              null
            }
          })
    }
  }
}

class KuduSubscriber(val key: String)(implicit val center: TeleporterCenter) extends SubscriberSupport[AsyncKuduClient] {
  override def workProps: Props = {
    Props(classOf[KuduSubscriberWork], client).withDispatcher("akka.teleporter.blocking-io-dispatcher")
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

object KuduComponent {
  val kuduClient: ClientApply = (key, center) ⇒ {
    val addressContext = center.context.getContext[AddressContext](key)
    val kuduMetaBean = addressContext.config.mapTo[KuduMetaBean]
    val asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMetaBean.kuduMaster)
      .workerCount(kuduMetaBean.workerCount).build()
    AutoCloseClientRef(key, asyncClient)
  }
}
