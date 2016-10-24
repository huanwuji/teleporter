package teleporter.integration.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.stream.actor.ActorSubscriberMessage.OnNext
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.integration._
import teleporter.integration.component._
import teleporter.integration.core._
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{MapBean, MapMetadata}

import scala.concurrent.{ExecutionContext, Future}

/**
  * author: huanwuji
  * created: 2015/8/2.
  */
trait DataSourcePublisherMetadata extends MapMetadata {
  val FSql = "sql"
}

class JdbcPublisher(override val key: String)(implicit override val center: TeleporterCenter)
  extends ScheduleActorPublisher[JdbcMessage, DataSource] with SqlSupport
    with SourceMetadata with DataSourcePublisherMetadata {
  override implicit val executionContext: ExecutionContext = context.dispatcher
  var sqlResult: SqlResult[Iterator[Map[String, Any]]] = _

  override protected def grab(config: MapBean): Future[Iterator[JdbcMessage]] = {
    Future {
      val namedSql = NameSql(config[String](FClient, FSql), config[MapBean](FClient).toMap)
      sqlResult = bulkQueryToMap(client.getConnection, PreparedSql(namedSql))
      sqlResult.result
    }
  }

  private def resourceClose() =
    if (sqlResult != null) {
      sqlResult.close()
    }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    logger.info(s"$key jdbc publisher was stop")
    resourceClose()
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

class JdbcSubscriber(val key: String)(implicit val center: TeleporterCenter) extends SubscriberSupport[DataSource] {
  override def createHandler: SubscriberHandler[DataSource] = new JdbcSubscriberHandler(client)
}

object DataSourceComponent extends AddressMetadata {
  def dataSourceApply: ClientApply = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config
    val props = new Properties()
    lnsClient(config).toMap.foreach {
      case (k, v) ⇒
        if (v != null && v.toString.nonEmpty) {
          props.put(k, v.toString)
        }
    }
    val hikariConfig = new HikariConfig(props)
    AutoCloseClientRef(key, new HikariDataSource(hikariConfig))
  }
}