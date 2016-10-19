package teleporter.integration.component.mongo

import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import teleporter.integration._
import teleporter.integration.component.{MongoMessage, ScheduleActorPublisher}
import teleporter.integration.core._
import teleporter.integration.script.Template
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{MapBean, MapMetadata}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by joker on 15/12/07
  */
trait MongoAddressMetadata extends MapMetadata {
  val FUrl = "url"
}

trait MongoPublisherMetadata extends MongoAddressMetadata with SourceMetadata {
  val FDatabase = "database"
  val FCollection = "collection"
  val FQuery = "query"

  def lnsDatabase(implicit config: MapBean): String = config[String](FClient, FDatabase)

  def lnsCollection(implicit config: MapBean): String = config[String](FClient, FCollection)
}

class MongoPublisher(override val key: String)(implicit val center: TeleporterCenter)
  extends ScheduleActorPublisher[MongoMessage, MongoClient]
    with MongoPublisherMetadata {
  override implicit val executionContext: ExecutionContext = context.dispatcher
  var collection: MongoCollection[Document] = _

  override protected def grab(config: MapBean): Future[Iterator[Document]] = {
    if (collection == null) {
      val database = client.getDatabase(lnsDatabase(config))
      collection = database.getCollection(lnsCollection(config))
    }
    val scheduleConfig = config[MapBean](FSchedule)
    val filter = scheduleConfig.__dict__[String](FQuery).map(s ⇒ Document(Template(s, scheduleConfig.toMap))).getOrElse(Document())
    val query = collection.find(filter)
    if (isPageRoller()(config)) {
      query.skip(scheduleConfig[Int](FOffset)).limit(scheduleConfig[Int](FPageSize))
    }
    query.toFuture().map(_.toIterator)
  }
}

object MongoComponent extends AddressMetadata with MongoAddressMetadata {
  val mongo: ClientApply[MongoClient] = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config
    val mongoClient = MongoClient(config[String](FClient, FUrl))
    new AutoCloseClientRef[MongoClient](key, mongoClient)
  }
}