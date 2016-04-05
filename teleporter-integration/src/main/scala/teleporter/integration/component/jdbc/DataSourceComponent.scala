package teleporter.integration.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.stream.actor.ActorSubscriberMessage.OnNext
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.integration.StreamControl.NotifyStream
import teleporter.integration.component._
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core._

import scala.collection.JavaConversions._

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
/**
 * @param conf address://hikari/jdbcUrl=mysql:jdbc://....
 */
class DataSourceAddressBuilder(override val conf: Conf.Address)(implicit override val center: TeleporterCenter)
  extends AddressBuilder[HikariDataSource] with PropsSupport {
  override def build: Address[HikariDataSource] = {
    val props = new Properties()
    props.putAll(cmpProps(conf.props).mapValues(_.toString).filter {
      case entry@(k, v) ⇒ v.nonEmpty
    })
    val config = new HikariConfig(props)
    new AutoCloseAddress[HikariDataSource](conf, new HikariDataSource(config))
  }
}

class JdbcPublisher(override val id: Int)(implicit override val center: TeleporterCenter)
  extends SchedulePublisher[JdbcMessage, DataSource] with SqlSupport with PropsSupport {

  import teleporter.integration.conf.DataSourceSourcePropsConversions._

  var sqlResult: SqlResult[Iterator[Map[String, Any]]] = _

  def handler(props: Conf.Props): Iterator[JdbcMessage] = {
    streamContext.currParallelismSource.incrementAndGet()
    val namedSql = NameSql(currConf.props.sql, currConf.props.cmpProps)
    sqlResult = bulkQueryToMap(address.getConnection, PreparedSql(namedSql), () ⇒ {
      if (streamContext.currParallelismSource.decrementAndGet() == 0) {
        streamManager ! NotifyStream(currConf.streamId)
      }
    })
    sqlResult.result
  }

  override def errorProcess(errorId: String, cause: Exception): Unit = {
    super.errorProcess(errorId, cause)
    resourceClose()
  }

  private def resourceClose() =
    if (sqlResult != null) {
      sqlResult.close()
    }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    logger.info("jdbc publisher was stop")
    resourceClose()
    center.removeAddress(currConf.addressId.get, currConf)
  }
}

class JdbcSubscriberHandler(val client: DataSource) extends SubscriberHandler[DataSource] with SqlSupport {
  override def handle(onNext: OnNext): Unit = {
    onNext.element match {
      case ele: TeleporterJdbcRecord ⇒
        ele.data.foreach(doAction(_, client))
        ele.toNext(ele)
      case ele: TeleporterJdbcFunction ⇒
        ele.data(client)
        ele.toNext(ele)
    }
  }
}

class JdbcSubscriber(override val id: Int)(implicit val center: TeleporterCenter) extends SubscriberSupport[DataSource] {
  override def createHandler: SubscriberHandler[DataSource] = new JdbcSubscriberHandler(client)
}