package teleporter.integration.component.mongo

import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import teleporter.integration._
import teleporter.integration.component.ScheduleActorPublisherMessage.ScheduleSetting
import teleporter.integration.component.{MongoMessage, ScheduleActorPublisher, ScheduleMetaBean}
import teleporter.integration.core._
import teleporter.integration.script.Template
import teleporter.integration.utils.Converters._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by joker on 15/12/07
  */
object MongoAddressMetaBean {
  val FUrl = "url"
}

class MongoAddressMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import MongoAddressMetaBean._

  def url: String = client[String](FUrl)
}

object MongoPublisherMetaBean {
  val FDatabase = "database"
  val FCollection = "collection"
  val FQuery = "query"
}

class MongoPublisherMetaBean(override val underlying: Map[String, Any]) extends ScheduleMetaBean(underlying) {

  import MongoPublisherMetaBean._

  def database: String = client[String](FDatabase)

  def collection: String = client[String](FCollection)

  def query: Option[String] = client.get[String](FQuery)
}

class MongoPublisher(override val key: String)(implicit val center: TeleporterCenter)
  extends ScheduleActorPublisher[MongoMessage, MongoClient] {
  override implicit val executionContext: ExecutionContext = context.dispatcher
  var collection: MongoCollection[Document] = _

  override protected def grab(scheduleSetting: ScheduleSetting): Future[Iterator[Document]] = {
    val config = scheduleSetting.scheduleMetaBean.mapTo[MongoPublisherMetaBean]
    if (collection == null) {
      val database = client.getDatabase(config.database)
      collection = database.getCollection(config.collection)
    }
    val filter = config.query.map(s ⇒ Document(Template(s, config.schedule.toMap))).getOrElse(Document())
    val query = collection.find(filter)
    scheduleSetting.pageAttrs match {
      case Some(pageAttrs) ⇒
        query.skip(pageAttrs.offset).limit(pageAttrs.pageSize)
      case _ ⇒
    }
    query.toFuture().map(_.toIterator)
  }
}

object MongoComponent {
  def mongoApply: ClientApply = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config
    val mongoMetaBean = config.client.mapTo[MongoAddressMetaBean]
    val mongoClient = MongoClient(config[String](mongoMetaBean.url))
    new AutoCloseClientRef[MongoClient](key, mongoClient)
  }
}