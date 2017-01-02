package teleporter.integration.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.actor.Props
import akka.stream.actor.ActorSubscriberMessage.OnNext
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.integration._
import teleporter.integration.component.ScheduleActorPublisherMessage.ScheduleSetting
import teleporter.integration.component._
import teleporter.integration.core._

import scala.concurrent.{ExecutionContext, Future}

/**
  * author: huanwuji
  * created: 2015/8/2.
  */
object JdbcPublisherMetaBean {
  val FSql = "sql"
}

class JdbcPublisherMetaBean(override val underlying: Map[String, Any]) extends ScheduleMetaBean(underlying) {

  import JdbcPublisherMetaBean._

  def sql: String = client[String](FSql)
}

class JdbcPublisher(override val key: String)(implicit override val center: TeleporterCenter)
  extends ScheduleActorPublisher[JdbcMessage, DataSource] with SqlSupport {
  override implicit val executionContext: ExecutionContext = context.dispatcher
  var sqlResult: SqlResult[Iterator[Map[String, Any]]] = _

  override protected def grab(scheduleSetting: ScheduleSetting): Future[Iterator[JdbcMessage]] = {
    Future {
      val config = scheduleSetting.scheduleMetaBean.mapTo[JdbcPublisherMetaBean]
      val namedSql = NameSql(config.sql, config.schedule.toMap)
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

class JdbcSubscriberWork(val client: DataSource) extends SubscriberWorker[DataSource] with SqlSupport {
  override def handle(onNext: OnNext, nrOfRetries: Int): Unit = {
    onNext.element match {
      case ele: TeleporterJdbcRecord ⇒
        ele.data.foreach(doAction(_, client))
        success(onNext)
      case ele: TeleporterJdbcFunction ⇒
        ele.data(client)
        ele.confirmed(ele)
    }
  }
}

class JdbcSubscriber(val key: String)(implicit val center: TeleporterCenter) extends SubscriberSupport[DataSource] {
  override def workProps: Props = Props(classOf[JdbcSubscriberWork], client).withDispatcher("akka.teleporter.blocking-io-dispatcher")
}

object JdbcComponent {
  def jdbcApply: ClientApply = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config
    val props = new Properties()
    config.client.toMap.foreach {
      case (k, v) ⇒
        if (v != null && v.toString.nonEmpty) {
          props.put(k, v.toString)
        }
    }
    val hikariConfig = new HikariConfig(props)
    AutoCloseClientRef(key, new HikariDataSource(hikariConfig))
  }
}