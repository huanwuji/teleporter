package teleporter.integration.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.{Done, NotUsed}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.integration.component.Roller.RollerContext
import teleporter.integration.component._
import teleporter.integration.core._
import teleporter.integration.utils.MapBean

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
  * author: huanwuji
  * created: 2015/8/2.
  */
object Jdbc {
  def sourceAck(sourceKey: String)
               (implicit center: TeleporterCenter): Source[AckMessage[MapBean, JdbcMessage], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    source(sourceKey).map(m ⇒ SourceMessage(RollerContext.merge(sourceContext.config, m.coordinate), m.data))
      .via(SourceAck.flow[JdbcMessage](sourceContext.id, sourceContext.config))
  }

  def source(sourceKey: String)(implicit center: TeleporterCenter): Source[SourceMessage[RollerContext, JdbcMessage], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val sourceConfig = sourceContext.config.mapTo[JdbcSourceMetaBean]
    val bind = Option(sourceConfig.addressBind).getOrElse(sourceKey)
    val addressKey = sourceContext.address().key
    Source.fromGraph(new JdbcSource(
      sql = sourceConfig.sql,
      rollerContext = RollerContext(sourceContext.config),
      _create = () ⇒ center.context.register(addressKey, bind, () ⇒ address(addressKey)).client,
      _close = {
        _ ⇒
          center.context.unRegister(sourceKey, bind)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sourceKey, sourceContext.config)))
  }

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[JdbcRecord], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[JdbcRecord], Message[JdbcRecord], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[JdbcSinkMetaBean]
    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new JdbcSink(
      parallelism = sinkConfig.parallelism,
      _create = (ec) ⇒ Future {
        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
      }(ec),
      _close = {
        (_, _) ⇒
          center.context.unRegister(sinkKey, bind)
          Future.successful(Done)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
  }

  def address(addressKey: String)(implicit center: TeleporterCenter): AutoCloseClientRef[HikariDataSource] = {
    val config = center.context.getContext[AddressContext](addressKey).config
    val props = new Properties()
    config.client.toMap.foreach {
      case (k, v) if v != null && v.toString.nonEmpty ⇒
        props.put(k, v.toString)
    }
    val hikariConfig = new HikariConfig(props)
    AutoCloseClientRef(addressKey, new HikariDataSource(hikariConfig))
  }
}

object JdbcSourceMetaBean {
  val FSql = "sql"
}

class JdbcSourceMetaBean(override val underlying: JdbcMessage) extends SourceMetaBean(underlying) {

  import JdbcSourceMetaBean._

  def sql: String = client[String](FSql)
}

object JdbcSinkMetaBean {
  val FParallelism = "parallelism"
}

class JdbcSinkMetaBean(override val underlying: JdbcMessage) extends SinkMetaBean(underlying) {

  import JdbcSinkMetaBean._

  def parallelism: Int = client.get[Int](FParallelism).getOrElse(1)
}

class JdbcSource(sql: String, rollerContext: RollerContext, _create: () ⇒ DataSource, _close: DataSource ⇒ Unit)
  extends RollerSource[DataSource, SourceMessage[RollerContext, JdbcMessage]]("jdbc.source", rollerContext) with SqlSupport {

  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher

  var sqlResult: SqlResult[Iterator[JdbcMessage]] = _
  var it: Iterator[JdbcMessage] = _

  override def readData(client: DataSource, rollerContext: RollerContext): Option[SourceMessage[RollerContext, JdbcMessage]] = {
    try {
      if (sqlResult == null) {
        val namedSql = NameSql(sql, rollerContext.toMap)
        sqlResult = bulkQueryToMap(client.getConnection, PreparedSql(namedSql))
        it = sqlResult.result
      }
      if (it.hasNext) Some(SourceMessage(rollerContext, it.next())) else {
        sqlResult.close()
        sqlResult = null
        None
      }
    } catch {
      case NonFatal(ex) ⇒ sqlResult.close(); throw ex
    }
  }

  override def create(): DataSource = _create()

  override def close(client: DataSource): Unit = {
    if (sqlResult != null) {
      sqlResult.close()
    }
    _close(client)
  }
}

class JdbcSink(parallelism: Int, _create: (ExecutionContext) ⇒ Future[DataSource],
               _close: (DataSource, ExecutionContext) ⇒ Future[Done])(implicit val center: TeleporterCenter)
  extends CommonSinkAsyncUnordered[DataSource, Message[JdbcRecord], Message[JdbcRecord]]("jdbc.sink", parallelism) with SqlSupport {
  override def create(executionContext: ExecutionContext): Future[DataSource] = _create(executionContext)

  override def write(client: DataSource, elem: Message[JdbcRecord], executionContext: ExecutionContext): Future[Message[JdbcRecord]] = {
    implicit val ec = executionContext
    Future {
      elem.data.foreach(doAction(client, _))
      elem
    }(executionContext)
  }

  override def close(client: DataSource, executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}