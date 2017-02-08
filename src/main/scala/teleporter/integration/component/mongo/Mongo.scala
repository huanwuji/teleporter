package teleporter.integration.component.mongo

import akka.stream.scaladsl.Source
import akka.stream.{Attributes, TeleporterAttributes}
import akka.{Done, NotUsed}
import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import teleporter.integration.component.Roller.RollerContext
import teleporter.integration.component._
import teleporter.integration.core._
import teleporter.integration.script.Template
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.MapBean

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by joker on 15/12/07
  */
object Mongo {
  def sourceAck(sourceKey: String)
               (implicit center: TeleporterCenter): Source[AckMessage[MapBean, MongoMessage], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    source(sourceKey).mapConcat(m ⇒ m.data.map { d ⇒
      SourceMessage(RollerContext.merge(sourceContext.config, m.coordinate), d)
    }.toIndexedSeq)
      .via(SourceAck.flow[MongoMessage](sourceContext.id, sourceContext.config))
  }

  def source(sourceKey: String)
            (implicit center: TeleporterCenter): Source[SourceMessage[RollerContext, Seq[MongoMessage]], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val mongoSourceConfig = sourceContext.config.mapTo[MongoSourceMetaBean]
    val bind = Option(sourceContext.config.addressBind).getOrElse(sourceKey)
    val addressKey = sourceContext.address().key
    Source.fromGraph(new MongoSourceAsync(
      filter = mongoSourceConfig.filter,
      rollerContext = RollerContext(sourceContext.config),
      _create = (ec) ⇒ Future {
        val mongoClient = center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
        val database = mongoClient.getDatabase(mongoSourceConfig.database)
        database.getCollection(mongoSourceConfig.collection)
      }(ec),
      _close = {
        (_, _) ⇒
          center.context.unRegister(sourceKey, bind)
          Future.successful(Done)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sourceKey, sourceContext.config)))
  }

  def address(key: String)(implicit center: TeleporterCenter): AutoCloseClientRef[MongoClient] = {
    val config = center.context.getContext[AddressContext](key).config
    val mongoMetaBean = config.client.mapTo[MongoAddressMetaBean]
    val mongoClient = MongoClient(config[String](mongoMetaBean.url))
    new AutoCloseClientRef[MongoClient](key, mongoClient)
  }
}

object MongoAddressMetaBean {
  val FUrl = "url"
}

class MongoAddressMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import MongoAddressMetaBean._

  def url: String = client[String](FUrl)
}

object MongoSourceMetaBean {
  val FDatabase = "database"
  val FCollection = "collection"
  val FFilter = "filter"
}

class MongoSourceMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {

  import MongoSourceMetaBean._

  def database: String = client[String](FDatabase)

  def collection: String = client[String](FCollection)

  def filter: Option[String] = client.get[String](FFilter)
}

class MongoSourceAsync(filter: Option[String],
                       rollerContext: RollerContext,
                       _create: (ExecutionContext) ⇒ Future[MongoCollection[MongoMessage]],
                       _close: (MongoCollection[MongoMessage], ExecutionContext) ⇒ Future[Done])
  extends RollerSourceAsync[SourceMessage[RollerContext, Seq[MongoMessage]], MongoCollection[MongoMessage]]("jdbc.source", rollerContext) {

  override def readData(client: MongoCollection[MongoMessage], rollerContext: RollerContext, executionContext: ExecutionContext)
  : Future[Option[SourceMessage[RollerContext, Seq[MongoMessage]]]] = {
    implicit val ec = executionContext
    val filterDoc = filter.map(s ⇒ Document(Template(s, rollerContext.toMap))).getOrElse(Document())
    val query = client.find(filterDoc)
    val filterQuery = rollerContext.pagination.map { page ⇒
      query.skip(page.offset.toInt).limit(page.pageSize)
    }.getOrElse(query)
    filterQuery.toFuture().map(m ⇒ Some(SourceMessage(rollerContext, m)))
  }

  override def create(executionContext: ExecutionContext): Future[MongoCollection[MongoMessage]] = _create(executionContext)

  override def close(client: MongoCollection[MongoMessage], executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}