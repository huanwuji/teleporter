package teleporter.integration.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.actor.{Actor, ActorLogging}
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorSubscriber, RequestStrategy}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.integration.component.{JdbcMessage, RequestStrategy, TeleporterJdbcRecord}
import teleporter.integration.conf.Conf.Props
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core._
import teleporter.integration.transaction.{RecoveryPoint, TransactionConf}

import scala.collection.JavaConversions._


/**
 * author: huanwuji
 * created: 2015/8/2.
 */
/**
 * @param conf address://hikari/jdbcUrl=mysql:jdbc://....
 */
class DataSourceAddressBuilder(override val conf: Conf.Address)(implicit override val teleporterCenter: TeleporterCenter) extends AddressBuilder[DataSource] with PropsSupport {
  override def build: Address[DataSource] = {
    val props = new Properties()
    props.putAll(cmpProps(conf.props).mapValues(_.toString))
    val config = new HikariConfig(props)
    new Address[DataSource] {
      override val _conf: Conf.Address = conf
      override val client: HikariDataSource = new HikariDataSource(config)

      override def close(): Unit = client.close()
    }
  }
}

class JdbcPublisher(override val id: Int)
                   (implicit teleporterCenter: TeleporterCenter) extends RollerPublisher[JdbcMessage]
with SqlSupport with PropsSupport {

  import teleporter.integration.conf.DataSourceSourcePropsConversions._

  override var conf = teleporterCenter.sourceFactory.loadConf(id)
  val dataSource = teleporterCenter.addressing[DataSource](conf.addressId.get).get
  override implicit val recoveryPoint: RecoveryPoint[Conf.Source] = teleporterCenter.defaultRecoveryPoint
  override val transactionConf: TransactionConf = TransactionConf(conf)
  var sqlResult: SqlResult[Iterator[Map[String, Any]]] = null

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"Jdbc publisher preRestart: ${reason.getLocalizedMessage}", reason.getLocalizedMessage)
    super.preRestart(reason, message)
  }

  def handler(props: Props): Iterator[JdbcMessage] = {
    val namedSql = NameSql(conf.props.sql, conf.props)
    sqlResult = bulkQueryToMap(dataSource.getConnection, PreparedSql(namedSql))
    sqlResult.result
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    logger.info("jdbc publisher was stop")
    if (sqlResult != null) {
      sqlResult.close()
    }
    teleporterCenter.removeAddress(conf.addressId.get)
  }
}

class JdbcSubscriber(override val id: Int)
                    (implicit teleporterCenter: TeleporterCenter)
  extends ActorSubscriber with Component with SqlSupport with PropsSupport with ActorLogging {

  val conf = teleporterCenter.sinkFactory.loadConf(id)
  val dataSource: DataSource = teleporterCenter.addressing[DataSource](conf.addressId.get).get
  override protected val requestStrategy: RequestStrategy = RequestStrategy(conf.props)

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"jdbc subscriber preRestart: ${reason.getLocalizedMessage}", reason.getLocalizedMessage)
    super.preRestart(reason, message)
  }

  override def receive: Actor.Receive = {
    case OnNext(element: TeleporterJdbcRecord) ⇒
      val record = element.data
      record.foreach(doAction(_, dataSource))
      element.toNext(element)
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = teleporterCenter.removeAddress(conf.addressId.get)
}